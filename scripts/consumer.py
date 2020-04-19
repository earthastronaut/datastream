#!python
import argparse
import logging
import json

import datastream

parser = argparse.ArgumentParser(description="Simple Record Consumer")
parser.add_argument(
    'topic',
    help='Name of the topic.',
)
parser.add_argument(
    '-c', '--consumer-group-name',
    default='consumer_1',
    help='Name for consumer group. Consumers of a group can be parallelized.',
)
parser.add_argument(
    '-m', '--max-poll-records',
    default=10,
    help='Max number of records per poll.',
)
parser.add_argument(
    '--disable-auto-commit',
    action='store_false',
    help='Disable auto commits. Usually only needed when using consumer in code.',  # noqa
)
parser.add_argument(
    '-a', '--auto-commit-interval-size',
    default=10,
    help=""" Number of records to yield before
    doing a commit of the records. 

    If auto_commit_interval_size <= max_poll_records then it will
    iterate infinitely, polling for records every max_poll_records
    and then committing every auto_commit_interval_size.

    If auto_commit_interval_size > max_poll_records then it will
    iterate until max_poll_records or no more records found. 
    """,
)
parser.add_argument(
    '-l', '--loglevel',
    help='Log level: 0 (show all) < debug < info < warning < error < critical < NOSET.',  # noqa
)


def record_processor(record):
    print('Processing Record:', record['record_id'], record['data'])


if __name__ == '__main__':
    pargs = parser.parse_args()

    # 0 (show all) < debug < info < warning < error < critical < NOSET
    logging.basicConfig(level=pargs.loglevel)

    consumer = datastream.Consumer(
        topic=pargs.topic,
        consumer_group_name=pargs.consumer_group_name,
        max_poll_records=pargs.max_poll_records,
        enable_auto_commit=not pargs.disable_auto_commit,
        auto_commit_interval_size=pargs.auto_commit_interval_size,
    )
    print(f'Start consumer {consumer.consumer_group_name} for {consumer.topic}')
    with consumer:
        for record in consumer:
            record_processor(record)
