#!python
import argparse
import logging
import json

import datastream

parser = argparse.ArgumentParser(description="Produce records")
parser.add_argument(
    'topic',
    help='Name of the topic.',
)
parser.add_argument(
    '-d', '--data',
    default='{}',
    help='Json formatted string.',
)
parser.add_argument(
    '-k', '--key',
    default='',
    help='Key for the message. Note {i} can be used to substitute index.',
)
parser.add_argument(
    '-l', '--loglevel',
    help='Log level: 0 (show all) < debug < info < warning < error < critical < NOSET.'  # noqa
)
parser.add_argument(
    '-m', '--metadata',
    default='{}',
    help='Json formatted string.',
)
parser.add_argument(
    '-p', '--producer-name',
    default='producer_1',
    help='Name of the producer associated with the message.',
)
parser.add_argument(
    '-r', '--repeat',
    type=int,
    default=0,
    help='Number of times to repeat record.',
)

if __name__ == '__main__':
    pargs = parser.parse_args()

    logging.basicConfig(level=pargs.loglevel)

    producer = datastream.Producer(
        producer_name=pargs.producer_name,
    )

    data = json.loads(pargs.data)
    metadata = json.loads(pargs.metadata)

    record_ids = []
    for i in range(pargs.repeat):
        record_ids.append(
            producer.send(
                topic=pargs.topic,
                key=pargs.key.format(i=i),
                metadata=metadata,
                data=data,
            )
        )
    print(f'Produced records {record_ids}')
