-- Generated from Apache Flink 2.2.1's TableTestProgram "process-stateful-multi-input-with-timeout" (Apache-2.0).
CREATE TEMPORARY SYSTEM FUNCTION f AS 'org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils$TimedJoinFunction' LANGUAGE JAVA;

CREATE TABLE t (
  name STRING,
  score INT,
  ts TIMESTAMP_LTZ(3),
  WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/t',
  'format' = 'json'
);

CREATE TABLE city (
  name STRING,
  city STRING,
  ts TIMESTAMP_LTZ(3),
  WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/city',
  'format' = 'json'
);

CREATE TABLE sink (
  `name` STRING,
  `name0` STRING,
  `out` STRING,
  `rowtime` TIMESTAMP_LTZ(3)
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

