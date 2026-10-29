-- Generated from Apache Flink 2.2.1's TableTestProgram "temporal-join-table-join-nested-key" (Apache-2.0).
CREATE TABLE OrdersNestedId (
  amount bigint,
  nested_row ROW<currency STRING>,
  nested_map MAP<STRING NOT NULL, STRING>,
  order_time STRING,
  rowtime as TO_TIMESTAMP(order_time) ,
  WATERMARK FOR rowtime AS rowtime
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/OrdersNestedId',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE RatesHistory (
  currency STRING,
  rate bigint,
  rate_time STRING,
  rowtime as TO_TIMESTAMP(rate_time) ,
  WATERMARK FOR rowtime AS rowtime,
  PRIMARY KEY(currency) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/RatesHistory',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE MySink (
  amount bigint
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/MySink',
  'format' = 'debezium-json'
);

