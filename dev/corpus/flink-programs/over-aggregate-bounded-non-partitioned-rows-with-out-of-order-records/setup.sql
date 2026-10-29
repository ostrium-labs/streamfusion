-- Generated from Apache Flink 2.2.1's TableTestProgram "over-aggregate-bounded-non-partitioned-rows-with-out-of-order-records" (Apache-2.0).
CREATE TEMPORARY SYSTEM FUNCTION LTCNT AS 'org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions$LargerThanCount' LANGUAGE JAVA;

CREATE TABLE MyTable (
  ts bigint,
  a bigint,
  b int,
  c string,
  rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts)),
  watermark for rowtime as rowtime
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/MyTable',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE MySink (
  a string,
  ts bigint,
  rowtime TIMESTAMP(3),
  b int,
  c bigint,
  d bigint,
  e bigint
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/MySink',
  'format' = 'debezium-json'
);

