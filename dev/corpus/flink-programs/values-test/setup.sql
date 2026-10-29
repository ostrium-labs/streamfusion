-- Generated from Apache Flink 2.2.1's TableTestProgram "values-test" (Apache-2.0).
CREATE TABLE sink_t (
  b INT,
  a INT,
  c VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

