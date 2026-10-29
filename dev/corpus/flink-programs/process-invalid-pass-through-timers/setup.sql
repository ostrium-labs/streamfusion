-- Generated from Apache Flink 2.2.1's TableTestProgram "process-invalid-pass-through-timers" (Apache-2.0).
CREATE TEMPORARY SYSTEM FUNCTION f AS 'org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils$InvalidPassThroughTimersFunction' LANGUAGE JAVA;

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

