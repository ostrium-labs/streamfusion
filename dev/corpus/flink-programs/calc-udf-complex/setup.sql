-- Generated from Apache Flink 2.2.1's TableTestProgram "calc-udf-complex" (Apache-2.0).
CREATE TEMPORARY FUNCTION udf1 AS 'org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions$JavaFunc0' LANGUAGE JAVA;

CREATE TEMPORARY FUNCTION udf2 AS 'org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions$JavaFunc1' LANGUAGE JAVA;

CREATE TEMPORARY SYSTEM FUNCTION udf3 AS 'org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions$JavaFunc2' LANGUAGE JAVA;

CREATE TEMPORARY SYSTEM FUNCTION udf4 AS 'org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions$UdfWithOpen' LANGUAGE JAVA;

CREATE FUNCTION udf5 AS 'org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions$JavaFunc5' LANGUAGE JAVA;

CREATE TABLE source_t (
  a BIGINT, b INT NOT NULL, c VARCHAR, d TIMESTAMP(3)
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  a BIGINT,
  a1 VARCHAR,
  b INT NOT NULL,
  b1 VARCHAR,
  c1 VARCHAR,
  c2 VARCHAR,
  d1 TIMESTAMP(3)
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

