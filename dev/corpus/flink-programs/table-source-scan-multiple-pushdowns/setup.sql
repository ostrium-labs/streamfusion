-- Generated from Apache Flink 2.2.1's TableTestProgram "table-source-scan-multiple-pushdowns" (Apache-2.0).
CREATE TABLE source_t (
  a INT,
  b BIGINT,
  c VARCHAR,
  ts TIMESTAMP(3) METADATA,
  watermark for ts as ts - interval '1' second
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  a INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

