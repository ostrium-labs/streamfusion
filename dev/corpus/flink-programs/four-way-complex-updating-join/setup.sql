-- Generated from Apache Flink 2.2.1's TableTestProgram "four-way-complex-updating-join" (Apache-2.0).
CREATE TABLE Users (
  name STRING,
  cash INT,
  user_id_0 STRING PRIMARY KEY NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Users',
  'format' = 'debezium-json'
);

CREATE TABLE Orders (
  order_id STRING PRIMARY KEY NOT ENFORCED,
  product STRING,
  user_id_1 STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Orders',
  'format' = 'json'
);

CREATE TABLE Payments (
  payment_id STRING PRIMARY KEY NOT ENFORCED,
  price INT,
  user_id_2 STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Payments',
  'format' = 'json'
);

CREATE TABLE Shipments (
  location STRING,
  user_id_3 STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Shipments',
  'format' = 'debezium-json'
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

