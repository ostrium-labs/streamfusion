-- Generated from Apache Flink 2.2.1's TableTestProgram "three-way-join-with-time-attributes" (Apache-2.0).
CREATE TABLE UsersWithProctime (
  user_id_0 STRING PRIMARY KEY NOT ENFORCED, name STRING, proctime AS PROCTIME()
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/UsersWithProctime',
  'format' = 'json'
);

CREATE TABLE OrdersWithRowtime (
  order_id STRING PRIMARY KEY NOT ENFORCED,user_id_1 STRING,rowtime TIMESTAMP(3),WATERMARK FOR rowtime AS rowtime
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/OrdersWithRowtime',
  'format' = 'json'
);

CREATE TABLE Payments (
  payment_id STRING PRIMARY KEY NOT ENFORCED,price INT,user_id_2 STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Payments',
  'format' = 'json'
);

CREATE TABLE sink (
  name STRING,
  order_id STRING,
  rowtime TIMESTAMP(3),
  price INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

