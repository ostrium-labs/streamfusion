-- Generated from Apache Flink 2.2.1's TableTestProgram "three-way-inner-join-three-keys-shuffled" (Apache-2.0).
CREATE TABLE Users3K (
  k1 STRING,
  k2 INT,
  k3 BOOLEAN,
  name STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Users3K',
  'format' = 'json'
);

CREATE TABLE Orders3K (
  k2 INT PRIMARY KEY NOT ENFORCED,order_id STRING,k3 BOOLEAN,k1 STRING,k4 STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Orders3K',
  'format' = 'json'
);

CREATE TABLE Payments3K (
  k1 STRING,
  payment_id STRING,
  k3 BOOLEAN,
  k2 INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Payments3K',
  'format' = 'json'
);

CREATE TABLE sink (
  name STRING,
  order_id STRING,
  payment_id STRING,
  k1 STRING,
  k2 INT,
  k3 BOOLEAN
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

