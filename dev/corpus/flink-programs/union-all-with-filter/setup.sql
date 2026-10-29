-- Generated from Apache Flink 2.2.1's TableTestProgram "union-all-with-filter" (Apache-2.0).
CREATE TABLE source_t1 (
  a INT,
  b VARCHAR,
  c INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t1',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE source_t2 (
  a INT,
  b VARCHAR,
  c INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t2',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t1_union_all_t2 (
  a INT,
  b VARCHAR,
  c INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t1_union_all_t2',
  'format' = 'debezium-json'
);

