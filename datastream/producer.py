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


__all__ = ['Producer']


logger = logging.getLogger(__name__)


# ############################################################################ #
# Insert record
# ############################################################################ #


def execute_insert_record(
    cursor: cursor_type,
    producer_name: str,
    topic: str,
    key: str,
    metadata: bytes,
    data: bytes,
) -> int:
    """ Does a record insert without commit """
    sql = f"""
    INSERT INTO {config.DATASTREAM_SCHEMA}.record
    (topic, key, producer_name, metadata, data) 
    VALUES (%(topic)s, %(key)s, %(producer_name)s, %(metadata)s, %(data)s)
    RETURNING record_id
    """
    params = {
        'topic': topic,
        'key': key,
        'producer_name': producer_name,
        'metadata': metadata,
        'data': data,
    }
    if logger.level <= logging.DEBUG:
        # don't mogrify unless needed to log
        logger.debug(cursor.mogrify(sql, params).decode('utf-8'))

    cursor.execute(sql, params)
    row = cursor.fetchone()
    return row[0]


def send_record(
    connection: connection_type,
    producer_name: str,
    topic: str,
    key: str,
    metadata: bytes,
    data: bytes,
) -> int:
    """ Send a record to the backend

    This step happens serially so an id (offset) can be incremented for the
    topic (and partition--Not Implemented). Postgres is handling the
    serialization of that record index.

    Parameters:
        connection (psycopg2.extensions.connection): storage backend.
        producer_name (str): An identifier for the producer of the message.
        topic (str): topic name.
        key (str): key used for message. For purposes of deduplicating if
            cleaned up. Note that this is similar behavior to kafka where
            log compaction would do clean up. This is a string instead
            of byte array to make it human readable in the database.
        metadata (bytes): serialized metadata which accompanies the data.
        data (bytes): serialized data.

    Returns:
        (int): record id
    """
    with connection:
        with connection.cursor() as cursor:
            return execute_insert_record(
                cursor,
                producer_name=producer_name,
                topic=topic,
                key=key,
                data=data,
                metadata=metadata,
            )


# ############################################################################ #
# Producer Class
# ############################################################################ #


class Producer:
    """ Producer for sending messages to the data stream.

    Adds records to data stream topic. Doesn't do any checks before inserting.

    Parameters:
        producer_name (str): An identifier for the producer of the message.
        connection (psycopg2.extensions.connection): storage backend.        

    Usage:

        Send a message with default serializer.

        ```
        producer.send(
            topic='rabbit',
            key='turtle123',
            metadata={},
            data={'hello': 'world'},
        )    
        ```
    """

    @staticmethod
    def serializer(data):
        if isinstance(data, bytes):
            return data
        return json.dumps(data).encode('utf-8')

    def __init__(
        self,
        producer_name: str,
        connection: connection_type = None,
    ):
        self.producer_name = producer_name
        if connection is None:
            connection = database.create_connection()
        self.connection = connection

    def send(self, topic, data, key, metadata=None):
        metadata = metadata or {}
        return send_record(
            connection=self.connection,
            producer_name=self.producer_name,
            topic=topic,
            key=key,
            data=self.serializer(data),
            metadata=self.serializer(metadata),
        )
