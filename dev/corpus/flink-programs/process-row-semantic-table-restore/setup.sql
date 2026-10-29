-- Generated from Apache Flink 2.2.1's TableTestProgram "process-row-semantic-table-restore" (Apache-2.0).
CREATE TEMPORARY SYSTEM FUNCTION f AS 'org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils$RowSemanticTableFunction' LANGUAGE JAVA;

CREATE TABLE t (
  name STRING,
  score INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink (
  `out` STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

