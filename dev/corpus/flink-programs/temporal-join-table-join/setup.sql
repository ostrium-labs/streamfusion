-- Generated from Apache Flink 2.2.1's TableTestProgram "temporal-join-table-join" (Apache-2.0).
CREATE TABLE Orders (
  amount bigint,
  currency STRING,
  order_time STRING,
  rowtime as TO_TIMESTAMP(order_time) ,
  WATERMARK FOR rowtime AS rowtime
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Orders',
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

