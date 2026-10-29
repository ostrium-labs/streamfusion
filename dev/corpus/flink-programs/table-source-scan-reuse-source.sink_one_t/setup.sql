-- Generated from Apache Flink 2.2.1's TableTestProgram "table-source-scan-reuse-source" (Apache-2.0).
CREATE TABLE source_t (
  a INT,
  b BIGINT,
  c VARCHAR,
  d TIMESTAMP(3)
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_one_t (
  a INT,
  c VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_one_t',
  'format' = 'debezium-json'
);

CREATE TABLE sink_two_t (
  a INT,
  b BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_two_t',
  'format' = 'debezium-json'
);

