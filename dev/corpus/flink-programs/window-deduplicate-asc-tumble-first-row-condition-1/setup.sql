-- Generated from Apache Flink 2.2.1's TableTestProgram "window-deduplicate-asc-tumble-first-row-condition-1" (Apache-2.0).
CREATE TABLE bid_t (
  ts STRING,
  price DECIMAL(10,2),
  item STRING,
  supplier_id STRING,
  `bid_time` AS TO_TIMESTAMP(`ts`),
  `proc_time` AS PROCTIME(),
  WATERMARK for `bid_time` AS `bid_time` - INTERVAL '1' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/bid_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  bid_time TIMESTAMP(3),
  price DECIMAL(10,2),
  item STRING,
  supplier_id STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  row_num BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

