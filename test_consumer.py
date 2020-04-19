import logging

import datastream

# 0 (show all) < debug < info < warning < error < critical < NOSET
logging.basicConfig(level='ERROR')

consumer = datastream.Consumer(
    topic='rabbit',
    consumer_group_name='my_consumer_1',
    max_poll_records=10,
    enable_auto_commit=True,
    auto_commit_interval_size=10,
)

with consumer:
    for record in consumer:
        print('Processing!', record['record_id'], record['data'])
