-- Generated from Apache Flink 2.2.1's TableTestProgram "union-all-two-sources" (Apache-2.0).
CREATE TABLE source_t1 (
  a BIGINT,
  b INT NOT NULL,
  c VARCHAR,
  d TIMESTAMP(3)
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t1',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE source_t2 (
  d BIGINT,
  e INT NOT NULL
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t2',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t1_union_all_t2 (
  a BIGINT,
  b INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t1_union_all_t2',
  'format' = 'debezium-json'
);

