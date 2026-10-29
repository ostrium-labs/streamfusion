-- Generated from Apache Flink 2.2.1's TableTestProgram "insert-retract-without-pk" (Apache-2.0).
CREATE TABLE source_t (
  name STRING,
  score INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'json'
);

CREATE TABLE sink_t (
  name STRING,
  score BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

