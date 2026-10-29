-- Generated from Apache Flink 2.2.1's TableTestProgram "calc-simple" (Apache-2.0).
CREATE TABLE t (
  a BIGINT,
  b DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/t',
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

