-- Generated from Apache Flink 2.2.1's TableTestProgram "process-multi-input" (Apache-2.0).
CREATE TEMPORARY SYSTEM FUNCTION f AS 'org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils$MultiInputFunction' LANGUAGE JAVA;

CREATE VIEW t AS SELECT * FROM (VALUES ('Bob', 12), ('Alice', 42), ('Bob', 99), ('Bob', 100), ('Alice', 400)) AS T(name, score);

CREATE VIEW city AS SELECT * FROM (VALUES ('Bob', 'London'), ('Alice', 'Berlin'), ('Charly', 'Paris')) AS T(name, city);

CREATE TABLE sink (
  `name` STRING,
  `name0` STRING,
  `out` STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

