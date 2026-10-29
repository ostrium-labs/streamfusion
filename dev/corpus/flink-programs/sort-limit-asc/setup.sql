-- Generated from Apache Flink 2.2.1's TableTestProgram "sort-limit-asc" (Apache-2.0).
CREATE TABLE source_t (
  a INT,
  b VARCHAR,
  c INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  a INT,
  b VARCHAR,
  c BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

