-- Generated from Apache Flink 2.2.1's TableTestProgram "process-regular-timestamp" (Apache-2.0).
CREATE TEMPORARY SYSTEM FUNCTION f AS 'org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils$TimeConversionsFunction' LANGUAGE JAVA;

CREATE TABLE t (
  name STRING,
  score INT,
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '1' HOUR
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/t',
  'format' = 'json'
);

CREATE TABLE sink (
  `out` STRING, `rowtime` TIMESTAMP(3)
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

