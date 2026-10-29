-- Generated from Apache Flink 2.2.1's TableTestProgram "process-invalid-row-kind" (Apache-2.0).
CREATE TEMPORARY SYSTEM FUNCTION f AS 'org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils$InvalidRowKindFunction' LANGUAGE JAVA;

CREATE VIEW t AS SELECT * FROM (VALUES ('Bob', 12), ('Alice', 42)) AS T(name, score);

