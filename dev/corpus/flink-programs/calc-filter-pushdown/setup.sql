-- Generated from Apache Flink 2.2.1's TableTestProgram "calc-filter-pushdown" (Apache-2.0).
CREATE TABLE source_t (
  a BIGINT,
  b DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  a BIGINT,
  b DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

