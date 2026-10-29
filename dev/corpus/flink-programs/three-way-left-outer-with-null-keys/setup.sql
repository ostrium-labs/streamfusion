-- Generated from Apache Flink 2.2.1's TableTestProgram "three-way-left-outer-with-null-keys" (Apache-2.0).
CREATE VIEW UsersNulls AS SELECT * FROM (VALUES ('1','Gus'),(CAST(NULL AS STRING), 'NullUser')) AS T(user_id, name);

CREATE VIEW OrdersNulls AS SELECT * FROM (VALUES ('1','order1'),(CAST(NULL AS STRING), 'nullOrder')) AS T(user_id, order_id);

CREATE VIEW PaymentsNulls AS SELECT * FROM (VALUES ('1','payment1'),('1','payment3'),(CAST(NULL AS STRING), 'paymentNull')) AS T(user_id, payment_id);

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

