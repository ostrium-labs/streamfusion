-- Generated from Apache Flink 2.2.1's TableTestProgram "window-aggregate-hop-event-time-two-phase-distinct-split" (Apache-2.0).
CREATE TABLE window_source_t (
  ts STRING,
  a_int INT,
  b_double DOUBLE,
  c_float FLOAT,
  d_bigdec DECIMAL(10, 2),
  `comment` STRING,
  name STRING,
  `rowtime` AS TO_TIMESTAMP(`ts`),
  `proctime` AS PROCTIME(),
  WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/window_source_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE cdc_window_source_t (
  ts STRING,
  a_int INT,
  b_double DOUBLE,
  c_float FLOAT,
  d_bigdec DECIMAL(10, 2),
  `comment` STRING,
  name STRING,
  `rowtime` AS TO_TIMESTAMP(`ts`),
  `proctime` AS PROCTIME(),
  WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/cdc_window_source_t',
  'format' = 'debezium-json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE window_sink_t (
  name STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  cnt BIGINT,
  sum_int INT,
  distinct_cnt BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/window_sink_t',
  'format' = 'debezium-json'
);

