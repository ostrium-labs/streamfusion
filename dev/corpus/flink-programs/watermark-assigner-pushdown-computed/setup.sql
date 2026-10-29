-- Generated from Apache Flink 2.2.1's TableTestProgram "watermark-assigner-pushdown-computed" (Apache-2.0).
CREATE TABLE source_t (
  i INT,
  b BIGINT,
  s STRING,
  ts AS TO_TIMESTAMP_LTZ(b, 3),
  WATERMARK for ts AS ts - INTERVAL '1' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  i INT,
  s STRING,
  ts TIMESTAMP_LTZ(3),
  w TIMESTAMP_LTZ(3)
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

