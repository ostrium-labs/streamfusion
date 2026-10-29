-- Generated from Apache Flink 2.2.1's TableTestProgram "four-way-mixed-join-multi-key-types-shuffled" (Apache-2.0).
CREATE TABLE Users4K (
  k1 STRING PRIMARY KEY NOT ENFORCED,
  k2 INT,
  k3 BOOLEAN,
  k4 STRING,
  name STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Users4K',
  'format' = 'debezium-json'
);

CREATE TABLE Orders4K (
  k2 INT PRIMARY KEY NOT ENFORCED,
  order_id STRING,
  k3 BOOLEAN,
  k1 STRING,
  k4 STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Orders4K',
  'format' = 'json'
);

CREATE TABLE Payments4K (
  k1 STRING PRIMARY KEY NOT ENFORCED,payment_id STRING,k3 BOOLEAN,k2 INT,k4 STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Payments4K',
  'format' = 'json'
);

CREATE TABLE Shipments4K (
  k3 BOOLEAN PRIMARY KEY NOT ENFORCED,
  k4 STRING,
  ship_id STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Shipments4K',
  'format' = 'json'
);

CREATE TABLE sink (
  name STRING,
  order_id STRING,
  payment_id STRING,
  ship_id STRING,
  k1 STRING,
  k2 INT,
  k3 BOOLEAN,
  k4 STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

