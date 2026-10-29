-- Generated from Apache Flink 2.2.1's TableTestProgram "calc-current-watermark" (Apache-2.0).
CREATE TABLE t (
  name STRING,
  ts TIMESTAMP_LTZ(3),
  WATERMARK FOR ts AS ts
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/t',
  'format' = 'json'
);

CREATE TABLE sink_t (
  name STRING,
  ts TIMESTAMP_LTZ(3),
  w TIMESTAMP_LTZ(3)
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

