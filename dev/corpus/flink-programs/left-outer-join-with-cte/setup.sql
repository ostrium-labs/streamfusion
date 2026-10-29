-- Generated from Apache Flink 2.2.1's TableTestProgram "left-outer-join-with-cte" (Apache-2.0).
CREATE TABLE Users (
  user_id STRING PRIMARY KEY NOT ENFORCED, name STRING, cash INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Users',
  'format' = 'json'
);

CREATE TABLE Orders (
  user_id STRING,
  order_id STRING PRIMARY KEY NOT ENFORCED,
  product STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Orders',
  'format' = 'debezium-json'
);

CREATE TABLE Payments (
  user_id STRING, payment_id STRING PRIMARY KEY NOT ENFORCED, price INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Payments',
  'format' = 'json'
);

CREATE TABLE sink (
  user_id STRING,
  name STRING,
  cnt BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

