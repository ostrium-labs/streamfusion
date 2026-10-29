-- Generated from Apache Flink 2.2.1's TableTestProgram "process-updating-input-enforced-full-deletes" (Apache-2.0).
CREATE TEMPORARY SYSTEM FUNCTION f AS 'org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils$SetSemanticTableFullDeletesArgFunction' LANGUAGE JAVA;

CREATE TABLE t (
  name STRING PRIMARY KEY NOT ENFORCED,
  score INT NOT NULL
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/t',
  'format' = 'debezium-json'
);

CREATE TABLE sink (
  `name` STRING,
  `out` STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

