-- Generated from Apache Flink 2.2.1's TableTestProgram "agg-with-state-ttl-hint" (Apache-2.0).
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
  b BIGINT,
  cnt BIGINT,
  avg_a DOUBLE,
  min_c VARCHAR,
  PRIMARY KEY (b) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

