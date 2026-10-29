-- Generated from Apache Flink 2.2.1's TableTestProgram "process-updating-input-enforced-retract" (Apache-2.0).
CREATE TEMPORARY SYSTEM FUNCTION f AS 'org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils$SetSemanticTableRetractArgFunction' LANGUAGE JAVA;

CREATE VIEW t AS SELECT name, COUNT(*) FROM (VALUES ('Bob', 12), ('Alice', 42), ('Bob', 14)) AS T(name, score) GROUP BY name;

CREATE TABLE sink (
  `name` STRING,
  `out` STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

