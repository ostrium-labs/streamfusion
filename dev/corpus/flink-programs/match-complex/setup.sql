-- Generated from Apache Flink 2.2.1's TableTestProgram "match-complex" (Apache-2.0).
CREATE TABLE MyTable (
  symbol string,
  tstamp bigint,
  price int,
  tax int,
  proctime as PROCTIME()
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/MyTable',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE MySink (
  somestring string,
  a bigint,
  b bigint,
  c bigint
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/MySink',
  'format' = 'debezium-json'
);

