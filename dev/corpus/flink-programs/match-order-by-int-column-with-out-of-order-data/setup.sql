-- Generated from Apache Flink 2.2.1's TableTestProgram "match-order-by-int-column-with-out-of-order-data" (Apache-2.0).
CREATE TABLE MyEventTimeTable (
  ts STRING,
  price INT,
  sequence_num INT,
  `rowtime` AS TO_TIMESTAMP(`ts`),
  `proctime` AS PROCTIME(),
  WATERMARK for `rowtime` AS `rowtime` - INTERVAL '2' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/MyEventTimeTable',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE MySink (
  first bigint,
  last bigint,
  up bigint
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/MySink',
  'format' = 'debezium-json'
);

