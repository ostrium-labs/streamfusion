-- Generated from Apache Flink 2.2.1's TableTestProgram "process-multi-input-restore" (Apache-2.0).
CREATE TEMPORARY SYSTEM FUNCTION f AS 'org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils$MultiInputFunction' LANGUAGE JAVA;

CREATE TABLE t (
  name STRING,
  score INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE city (
  name STRING,
  city STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/city',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink (
  `name` STRING,
  `name0` STRING,
  `out` STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

