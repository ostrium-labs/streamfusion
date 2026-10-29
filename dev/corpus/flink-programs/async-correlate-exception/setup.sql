-- Generated from Apache Flink 2.2.1's TableTestProgram "async-correlate-exception" (Apache-2.0).
CREATE TEMPORARY FUNCTION func1 AS 'org.apache.flink.table.planner.plan.nodes.exec.stream.AsyncCorrelateTestPrograms$FailureThenSucceedSplit' LANGUAGE JAVA;

CREATE TABLE source_t (
  a BIGINT,
  b INT NOT NULL,
  c VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  a VARCHAR,
  b VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

