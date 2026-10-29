-- Generated from Apache Flink 2.2.1's TableTestProgram "correlate-with-literal-agg" (Apache-2.0).
CREATE TABLE source_t1 (
  a INTEGER,
  b BIGINT,
  c STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t1',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE source_t2 (
  d INTEGER,
  e BIGINT,
  f STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t2',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE source_t3 (
  i INTEGER,
  j BIGINT,
  k STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t3',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  b BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

