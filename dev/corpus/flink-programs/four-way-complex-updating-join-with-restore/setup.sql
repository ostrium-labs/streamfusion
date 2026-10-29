-- Generated from Apache Flink 2.2.1's TableTestProgram "four-way-complex-updating-join-with-restore" (Apache-2.0).
CREATE TABLE Users (
  name STRING,
  user_id_0 STRING PRIMARY KEY NOT ENFORCED,
  cash INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Users',
  'format' = 'debezium-json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE Orders (
  order_id STRING PRIMARY KEY NOT ENFORCED,
  product STRING,
  user_id_1 STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Orders',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE Payments (
  user_id_2 STRING,
  payment_id STRING PRIMARY KEY NOT ENFORCED,
  price INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Payments',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE Shipments (
  location STRING,
  user_id_3 STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Shipments',
  'format' = 'debezium-json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink (
  user_id STRING,
  name STRING,
  order_id STRING,
  payment_id STRING,
  location STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

