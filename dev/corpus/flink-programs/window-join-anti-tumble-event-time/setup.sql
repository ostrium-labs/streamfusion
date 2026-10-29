-- Generated from Apache Flink 2.2.1's TableTestProgram "window-join-anti-tumble-event-time" (Apache-2.0).
CREATE TABLE source_one_t (
  ts STRING,
  id STRING,
  num INT,
  name STRING,
  row_time AS TO_TIMESTAMP(`ts`),
  WATERMARK for `row_time` AS `row_time` - INTERVAL '1' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_one_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE source_two_t (
  ts STRING,
  id STRING,
  num INT,
  name STRING,
  row_time AS TO_TIMESTAMP(`ts`),
  WATERMARK for `row_time` AS `row_time` - INTERVAL '1' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_two_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  name STRING,
  L_id STRING,
  L_num INT,
  R_id STRING,
  R_num INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

