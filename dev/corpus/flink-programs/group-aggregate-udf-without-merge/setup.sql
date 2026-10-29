-- Generated from Apache Flink 2.2.1's TableTestProgram "group-aggregate-udf-without-merge" (Apache-2.0).
CREATE TEMPORARY SYSTEM FUNCTION my_sum1 AS 'org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions$VarSum1AggFunction' LANGUAGE JAVA;

CREATE FUNCTION my_avg AS 'org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions$WeightedAvg' LANGUAGE JAVA;

CREATE TEMPORARY SYSTEM FUNCTION my_sum2 AS 'org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions$VarSum2AggFunction' LANGUAGE JAVA;

CREATE TABLE source_t (
  a INT,
  b BIGINT,
  c INT,
  d VARCHAR,
  e BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  d BIGINT,
  s1 BIGINT,
  s2 BIGINT,
  s3 BIGINT,
  PRIMARY KEY (d) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

