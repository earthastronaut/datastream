CREATE SCHEMA datastream;

CREATE TABLE IF NOT EXISTS datastream.record (
  record_id SERIAL UNIQUE
  , topic VARCHAR(256)
  , key VARCHAR(256)
  , PRIMARY KEY (record_id, topic, key)
  , producer_name VARCHAR(256)
  , timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
  , metadata BYTEA
  , data BYTEA
);

CREATE INDEX IF NOT EXISTS 
  idx__datastream_record__topic_lower
  ON datastream.record (lower(topic));


CREATE TABLE datastream.consumer_job (
  consumer_job_id SERIAL UNIQUE
  , consumer_group_name VARCHAR(256) NOT NULL
  , record_id INTEGER REFERENCES datastream.record(record_id) NOT NULL
  , PRIMARY KEY (consumer_job_id, consumer_group_name, record_id)

  , job_started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
  , job_completed_at TIMESTAMP WITH TIME ZONE   
);
