import logging

import datastream

# 0 (show all) < debug < info < warning < error < critical < NOSET
logging.basicConfig(level='ERROR')

producer = datastream.Producer('turtle_producer')

data = {'hello': 'world\u0003\\u0000\u0000', 'simple_kafka': True}
metadata = {'encoding': 'utf-8'}
record_ids = []
for i in range(5):
    data['index'] = i
    record_ids.append(
        producer.send(
            topic='rabbit',
            key=f'turtle_id{i}',
            metadata=metadata,
            data=data,
        )
    )
logger.info(f'Produced records {record_ids}')
