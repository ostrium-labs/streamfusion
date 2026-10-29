-- Generated from Apache Flink 2.2.1's TableTestProgram "three-way-inner-join-with-restore" (Apache-2.0).
CREATE TABLE Users (
  user_id STRING,
  name STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Users',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE Orders (
  user_id STRING,
  order_id STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Orders',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE Payments (
  user_id STRING,
  payment_id STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Payments',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink (
  user_id STRING,
  name STRING,
  order_id STRING,
  payment_id STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

