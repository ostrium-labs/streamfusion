-- Generated from Apache Flink 2.2.1's TableTestProgram "group-aggregate-distinct-mini-batch" (Apache-2.0).
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
  e BIGINT,
  cnt_a1 BIGINT,
  cnt_a2 BIGINT,
  sum_a BIGINT,
  sum_b BIGINT,
  avg_b DOUBLE,
  cnt_d BIGINT,
  PRIMARY KEY (e) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

