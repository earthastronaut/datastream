from datetime import datetime
import json
import logging
import time
from typing import (
    List,
    Dict,
)

from datastream import (
    database,
    config,
)
from datastream.database import (
    cursor_type,
    connection_type,
)


__all__ = ['Consumer']


logger = logging.getLogger(__name__)


# ############################################################################ #
# Fetch and Schedule Consumer Jobs
# ############################################################################ #

def execute_fetch_records(
    cursor: cursor_type,
    record_ids: List[int],
) -> List[Dict]:
    """ Get records to process without commit to job. """
    if not len(record_ids):
        return []

    record_ids_in = ', '.join(map(str, map(int, record_ids)))
    sql = f"""
    SELECT
        r.record_id
        , r.topic
        , r.key
        , r.timestamp
        , r.metadata
        , r.data
    FROM {config.DATASTREAM_SCHEMA}.record r
    WHERE r.record_id IN ({record_ids_in})
    """
    logger.debug(sql)

    cursor.execute(sql)
    column_names = [c.name for c in cursor.description]
    rows = [
        dict(zip(column_names, row))
        for row in cursor.fetchall()
    ]

    logger.info(f'Fetched {len(rows)} records')
    return rows


def execute_create_consumer_jobs(
    cursor: cursor_type,
    topic: str,
    consumer_group_name: str,
    max_records: int = 100,
    job_start_time: datetime = None,
) -> List[int]:
    """ Create consumer start jobs without commit. """
    sql = f"""
    INSERT INTO {config.DATASTREAM_SCHEMA}.consumer_job
    (consumer_group_name, record_id, job_started_at)
    (
        SELECT
            %(consumer_group_name)s
            , r.record_id
            , %(job_start_time)s
        FROM
            {config.DATASTREAM_SCHEMA}.record r
            LEFT JOIN {config.DATASTREAM_SCHEMA}.consumer_job cj ON
                r.record_id = cj.record_id
                AND consumer_group_name = %(consumer_group_name)s
        WHERE
            r.topic = %(topic)s
            AND cj.record_id IS NULL
        LIMIT %(max_records)s
    )
    RETURNING record_id
    """
    params = {
        'consumer_group_name': consumer_group_name,
        'job_start_time': job_start_time,
        'topic': topic,
        'max_records': max_records,
    }
    logger.debug(cursor.mogrify(sql, params).decode('utf-8'))

    cursor.execute(sql, params)
    rows = cursor.fetchall()

    logger.info(f'Created {len(rows)} consumer job rows')

    return [r[0] for r in rows]


def poll_records(
    connection: connection_type,
    consumer_group_name: str,
    topic: str,
    max_records: int = 100,
    lock_timeout_s: int = 5,
    job_start_time: datetime = None,
) -> List[Dict]:
    """ Get records to process.

    Gets multiple records to process per topic (and partition, Not Implemented).
    To be thread safe, this needs to serialize getting records and marking them 
    in progress with a job_started_at timestamp. Postgres currently handles 
    serializing these requests using the SERIALIZABLE transactions. This will 
    also protect against jobs being committed. 

    Note: Currently only for (topic, consumer group). For kafka, there is also
    a partition and consumer id which represents one serialized tasks within
    the parallelized (topic, consumer group). Kafka will often abstract this
    from the user by creating consumer ids and assigning them to partitions
    as long as the developer doesn't create more consumers than partitions. 
    In this implementation, scheduling of parallel consumers is handled by
    marking tasks in progress with job_started_at timestamp. So the scheduling
    of concurrent consumers per (topic, consumer group) is handled by a 
    global lock on the table. 

    Parameters:
        connection (psycopg2.extensions.connection): storage backend
        consumer_group_name (str): Name of consumer group
        topic (str): Topic for records
        max_records (int): Number of records to poll
        lock_timeout_s (int): Number of seconds before table lock timeout.
        job_start_time (datetime): used when marking jobs in progress.
            defaults to datetime.utcnow()

            Note: if a job crashes, then this sort of lock will be left on 
            the job and the record will not be processed. When this happens 
            you will need to delete all non-completed.

            ```
            DELETE FROM datastream.consumer_job
            WHERE
                topic = {topic}
                AND consumer_group_name = {consumer_group_name}
                AND job_completed_at IS NULL
                AND job_started_at IS NOT NULL
            ```

    Returns:
        (List[Dict]): list of the records

    """
    lock_timeout_s = max(int(lock_timeout_s), 1)
    job_start_time = job_start_time or datetime.utcnow()

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(f"LOCK TABLE {config.DATASTREAM_SCHEMA}.consumer_job IN ACCESS EXCLUSIVE MODE")  # noqa
            cursor.execute(f"SET LOCAL lock_timeout = '{lock_timeout_s}s'")
            record_ids = execute_create_consumer_jobs(
                cursor=cursor,
                topic=topic,
                consumer_group_name=consumer_group_name,
                max_records=max_records,
                job_start_time=job_start_time
            )

    with connection:
        with connection.cursor() as cursor:
            records = execute_fetch_records(
                cursor=cursor,
                record_ids=record_ids,
            )

    return records


# ############################################################################ #
# Poll for In-Progress Jobs
# ############################################################################ #

# Note:
#   These utilities are used for seeing what jobs have job_start_at but no
#   complete time. Especially useful for recovery from crashes. This record
#   list can be used to delete the job ids.

def poll_consumer_job_in_progress(
    connection: connection_type,
    topic: str,
    consumer_group_name: str,
) -> List[Dict]:
    """ Poll for in progress jobs """
    sql = f"""
    SELECT 
        r.record_id
        , r.topic
        , r.key
        , r.timestamp
        , consumer_group_name
        , consumer_job_id
        , job_started_at
    FROM 
        {config.DATASTREAM_SCHEMA}.consumer_job cj
        JOIN {config.DATASTREAM_SCHEMA}.record r ON r.record_id = cj.record_id
    WHERE
        r.topic = %(topic)s
        AND cj.consumer_group_name = %(consumer_group_name)s
        AND cj.job_completed_at IS NULL
    """
    params = {
        'topic': topic,
        'consumer_group_name': consumer_group_name,
    }
    logger.debug(cursor.mogrify(sql, params).decode('utf-8'))

    cursor.execute(sql, params)
    column_names = [c.name for c in cursor.description]
    rows = [
        dict(zip(column_names, row))
        for row in cursor.fetchall()
    ]

    logger.info(f'Created {len(rows)} consumer job rows')
    return rows


# ############################################################################ #
# Mark Jobs Complete
# ############################################################################ #


def execute_commit_records(
    cursor: cursor_type,
    consumer_group_name: str,
    record_ids: int,
    job_completed_at: datetime = None,
) -> List[int]:
    """ Updates consumer job record without commit """
    job_completed_at = job_completed_at or datetime.utcnow()
    record_ids_in = ', '.join(map(str, map(int, record_ids)))
    sql = f"""
    UPDATE {config.DATASTREAM_SCHEMA}.consumer_job
    SET
        job_completed_at = %(job_completed_at)s
    WHERE
        consumer_group_name = %(consumer_group_name)s
        AND record_id IN ({record_ids_in})
    RETURNING record_id
    """
    params = {
        'consumer_group_name': consumer_group_name,
        'job_completed_at': job_completed_at,
    }

    if logger.level <= logging.DEBUG:
        # don't mogrify unless needed to log
        logger.debug(cursor.mogrify(sql, params).decode('utf-8'))

    cursor.execute(sql, params)
    rows_update_consumer_job = cursor.fetchall()

    updated_record_ids = list((r[0] for r in rows_update_consumer_job))
    logger.info(f'Marked {len(updated_record_ids)} consumer jobs complete for consumer_group_name="{consumer_group_name}" ')  # noqa
    return updated_record_ids


def commit_records(
    connection: connection_type,
    consumer_group_name: str,
    record_ids: List[int],
    job_completed_at: datetime = None,
) -> List[int]:
    """ Marks records as completed with commit

    Marks the records as processing completed with a job_completed_at timestamp.
    Postgres default transaction handles concurrency with READ COMMITTED.

    Parameters:
        connection (psycopg2.extensions.connection): storage backend
        consumer_group_name (str): Name of consumer group
        record_ids (List[int]): List of record_id integers
        job_completed_at (datetime): defaults to datetime.utcnow()

    Returns:
        (List[int]): list of updated record ids
    """
    if not len(record_ids):
        return []

    with connection:
        with connection.cursor() as cursor:
            return execute_commit_records(
                cursor=cursor,
                consumer_group_name=consumer_group_name,
                record_ids=record_ids,
            )


# ############################################################################ #
# Consumer Class
# ############################################################################ #


class Consumer:
    """ Consumer for processing messages from the data stream

    See consumer.poll_records(...) for more details about how polling works.

    You can create multiple consumers and each will act as a thread/process
    safe consumer. Each consumer should process messages in serial. Give note
    to the connection argument and handle your database connections outside
    of this consumer.

    Parameters:
        topic (str): Topic to read from.
        consumer_group_name (str): Consumer group to track processing records
        max_poll_records (int): Number of records to fetch per poll.
        max_poll_interval_ms (int): Number of ms to wait between each fetch
            for more records.
        enable_auto_commit (bool): Autocommit after every 
            auto_commit_interval_size records you yield from iter(consumer).
        auto_commit_interval_size (int): Number of records to yield before
            doing a commit of the records. 

            If auto_commit_interval_size <= max_poll_records then it will
            iterate infinitely, polling for records every max_poll_records
            and then committing every auto_commit_interval_size.

            If auto_commit_interval_size > max_poll_records then it will
            iterate until max_poll_records or no more records found. 
        connection (psycopg2.extensions.connection): storage backend. This
            connection is used per process/thread. To make process/thread safe
            all consumers in a single process/thread should share a single
            connection object. None will generate a new connection to postgres.

    Usage:

        Declare object with default connection for current process/thread.

        ```
        consumer = Consumer(
            topic='topic_1',
            consumer_group_name='consumer_group_1',
            max_poll_records=10,
        ) 
        ```

        Use as context manager to commit any yielded messages. If exception
        then will commit all but the last yielded messaged otherwise will 
        commit all yielded messages.

        ```
        with consumer:
            for record in consumer:
                print('Processing!', record['record_id'], record['data'])
        ```

        Alternatively, you could commit after each processing.

        ```
        for record in consumer:
            print('Processing!', record['record_id'], record['data'])
            consumer.commit_records()
        ```

    """
    @staticmethod
    def deserializer(data):
        return json.dumps(data.tobytes().decode('utf-8'))

    def __init__(
        self,
        topic: str,
        consumer_group_name: str,
        max_poll_records: int = 100,
        max_poll_interval_ms: int = 500,
        enable_auto_commit: bool = False,
        auto_commit_interval_size: int = 1000,
        connection: connection_type = None,
        deserialize: bool = True,
    ):
        self.topic = topic
        self.consumer_group_name = consumer_group_name
        self.max_poll_records = max_poll_records
        self.max_poll_interval_ms = max_poll_interval_ms
        self.enable_auto_commit = enable_auto_commit
        self.auto_commit_interval_size = auto_commit_interval_size
        self.deserialize = deserialize
        self._yielded_record_ids = []

        if connection is None:
            connection = database.create_connection()
        self.connection = connection

    def commit_records(self, record_ids=None):
        if record_ids is None:
            response = commit_records(
                connection=self.connection,
                consumer_group_name=self.consumer_group_name,
                record_ids=self._yielded_record_ids,
            )
            self._yielded_record_ids = []
            return response
        else:
            return commit_records(
                connection=self.connection,
                consumer_group_name=self.consumer_group_name,
                record_ids=record_ids
            )

    def commit(self, record_id):
        self.commit_records(record_ids=[record_id])

    def poll_records(self, max_records=None):
        records = poll_records(
            consumer_group_name=self.consumer_group_name,
            topic=self.topic,
            max_records=(max_records or self.max_poll_records),
            connection=self.connection,
        )
        for record in records:
            if self.deserialize:
                record['data'], record['metadata'] = (
                    self.deserializer(record['data']),
                    self.deserializer(record['metadata']),
                )
            self._yielded_record_ids.append(record['record_id'])
            yield record

    def _check_autocommit(self):
        logger.info('Checking autocommit')
        if not self.enable_auto_commit:
            return

        if self.max_poll_records < self.auto_commit_interval_size:
            return

        if len(self._yielded_record_ids) >= self.auto_commit_interval_size:
            self.commit_records()

    def __iter__(self):
        while True:
            logger.info(f'Poll records for next {self.max_poll_records}')
            for record in self.poll_records():
                yield record

            if self.max_poll_records < self.auto_commit_interval_size:
                if self.enable_auto_commit:
                    self.commit_records()
                return

            self._check_autocommit()
            time.sleep(self.max_poll_interval_ms / 1000)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception, traceback):
        if exception is None:
            self.commit_records()
        else:
            self.commit_records(record_ids=self._yielded_record_ids[:-1])
            self._yielded_record_ids = []
