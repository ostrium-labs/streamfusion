-- Generated from Apache Flink 2.2.1's TableTestProgram "over-aggregate-non-time-rows-unbounded-sum-no-partition-by" (Apache-2.0).
CREATE TABLE source_t (
  key STRING,
  val BIGINT,
  ts BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  key STRING,
  val BIGINT,
  ts BIGINT,
  sum_val BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

