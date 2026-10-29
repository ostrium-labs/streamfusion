-- Generated from Apache Flink 2.2.1's TableTestProgram "process-updating-multi-input" (Apache-2.0).
CREATE TEMPORARY SYSTEM FUNCTION f AS 'org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils$UpdatingJoinFunction' LANGUAGE JAVA;

CREATE TABLE scores (
  name STRING PRIMARY KEY NOT ENFORCED,
  score INT NOT NULL
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/scores',
  'format' = 'debezium-json'
);

CREATE TABLE city (
  name STRING PRIMARY KEY NOT ENFORCED,
  city STRING NOT NULL
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/city',
  'format' = 'debezium-json'
);

CREATE TABLE sink (
  `name` STRING PRIMARY KEY NOT ENFORCED,
  `out` STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

