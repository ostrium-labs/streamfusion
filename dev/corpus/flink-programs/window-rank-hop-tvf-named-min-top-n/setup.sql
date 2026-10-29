-- Generated from Apache Flink 2.2.1's TableTestProgram "window-rank-hop-tvf-named-min-top-n" (Apache-2.0).
CREATE TABLE bid_t (
  ts STRING,
  price DECIMAL(10,2),
  supplier_id STRING,
  `bid_time` AS TO_TIMESTAMP(`ts`),
  WATERMARK for `bid_time` AS `bid_time` - INTERVAL '1' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/bid_t',
  'format' = 'json'
);

CREATE TABLE sink_t (
  bid_time TIMESTAMP(3),
  supplier_id STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

