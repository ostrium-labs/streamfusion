-- Generated from Apache Flink 2.2.1's TableTestProgram "async-calc-simple" (Apache-2.0).
CREATE TEMPORARY FUNCTION udf1 AS 'org.apache.flink.table.planner.plan.nodes.exec.stream.AsyncCalcTestPrograms$AsyncJavaFunc0' LANGUAGE JAVA;

CREATE TABLE source_t (
  a INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  a INT,
  a1 BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

