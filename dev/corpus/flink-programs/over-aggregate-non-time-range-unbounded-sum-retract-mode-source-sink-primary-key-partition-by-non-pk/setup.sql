-- Generated from Apache Flink 2.2.1's TableTestProgram "over-aggregate-non-time-range-unbounded-sum-retract-mode-source-sink-primary-key-partition-by-non-pk" (Apache-2.0).
CREATE TABLE source_t (
  key STRING,
  val BIGINT,
  ts BIGINT,
  PRIMARY KEY(val) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'debezium-json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  key STRING,
  val BIGINT,
  ts BIGINT,
  sum_val BIGINT,
  PRIMARY KEY(val) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

