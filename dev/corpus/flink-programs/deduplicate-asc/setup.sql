-- Generated from Apache Flink 2.2.1's TableTestProgram "deduplicate-asc" (Apache-2.0).
CREATE TABLE MyTable (
  order_id bigint,
  `user` varchar,
  product varchar,
  order_time bigint ,
  event_time as TO_TIMESTAMP(FROM_UNIXTIME(order_time)) ,
  watermark for event_time as event_time - INTERVAL '5' second 
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/MyTable',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE MySink (
  order_id bigint,
  `user` varchar,
  product varchar,
  order_time bigint,
  primary key(product) not enforced
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/MySink',
  'format' = 'debezium-json'
);

