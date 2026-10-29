-- Generated from Apache Flink 2.2.1's TableTestProgram "sink-ndf-primary-key" (Apache-2.0).
CREATE TEMPORARY FUNCTION ndf AS 'org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions$NonDeterministicUdf' LANGUAGE JAVA;

CREATE TABLE source_t (
  a INT,
  b BIGINT,
  c VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  a INT,
  b BIGINT,
  c VARCHAR,
  PRIMARY KEY(c) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

