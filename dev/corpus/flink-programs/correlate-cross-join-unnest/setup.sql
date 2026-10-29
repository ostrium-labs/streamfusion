-- Generated from Apache Flink 2.2.1's TableTestProgram "correlate-cross-join-unnest" (Apache-2.0).
CREATE TABLE source_t (
  name STRING,
  arr ARRAY<ROW<nested STRING>>
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  name STRING,
  nested STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

