-- Generated from Apache Flink 2.2.1's TableTestProgram "process-updating-output-full-deletes" (Apache-2.0).
CREATE TEMPORARY SYSTEM FUNCTION f AS 'org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils$UpdatingUpsertFunction' LANGUAGE JAVA;

CREATE TABLE t (
  `name` STRING PRIMARY KEY NOT ENFORCED,
  `score` BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/t',
  'format' = 'debezium-json'
);

CREATE TABLE sink (
  `name` STRING PRIMARY KEY NOT ENFORCED,
  `name0` STRING,
  `count` BIGINT,
  `mode` STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

