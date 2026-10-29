-- Generated from Apache Flink 2.2.1's TableTestProgram "process-scalar-args" (Apache-2.0).
CREATE TEMPORARY SYSTEM FUNCTION f AS 'org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils$ScalarArgsFunction' LANGUAGE JAVA;

CREATE TABLE sink (
  `out` STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

