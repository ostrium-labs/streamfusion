-- Generated from Apache Flink 2.2.1's TableTestProgram "interval-join-negative-interval" (Apache-2.0).
CREATE TABLE orders_t (
  id INT,
  order_ts_str STRING,
  order_ts AS TO_TIMESTAMP(order_ts_str),
  WATERMARK for `order_ts` AS `order_ts` - INTERVAL '1' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/orders_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE shipments_t (
  id INT,
  order_id INT,
  shipment_ts_str STRING,
  shipment_ts AS TO_TIMESTAMP(shipment_ts_str),
  WATERMARK for `shipment_ts` AS `shipment_ts` - INTERVAL '1' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/shipments_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  order_id INT,
  order_ts_str STRING,
  shipment_ts_str STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

