-- Generated from Apache Flink 2.2.1's TableTestProgram "group-aggregate-udf-with-merge-mini-batch" (Apache-2.0).
CREATE FUNCTION my_avg AS 'org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions$WeightedAvgWithMerge' LANGUAGE JAVA;

CREATE TEMPORARY SYSTEM FUNCTION my_concat AS 'org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions$ConcatDistinctAggFunction' LANGUAGE JAVA;

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
  c1 VARCHAR,
  PRIMARY KEY (d) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

