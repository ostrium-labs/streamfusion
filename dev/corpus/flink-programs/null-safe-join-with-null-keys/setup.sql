-- Generated from Apache Flink 2.2.1's TableTestProgram "null-safe-join-with-null-keys" (Apache-2.0).
CREATE VIEW UsersNullSafe AS SELECT * FROM (VALUES ('1','Gus'),(CAST(NULL AS STRING), 'NullUser')) AS T(user_id, name);

CREATE VIEW OrdersNullSafe AS SELECT * FROM (VALUES ('1','order1'),(CAST(NULL AS STRING), 'nullOrder')) AS T(user_id, order_id);

CREATE TABLE sink (
  user_id STRING,
  name STRING,
  order_id STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

