-- Generated from Apache Flink 2.2.1's TableTestProgram "rank-test-append-fast-strategy" (Apache-2.0).
CREATE TABLE MyTable (
  a INT,
  b VARCHAR,
  c INT primary key not enforced
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/MyTable',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  a INT,
  b VARCHAR,
  c BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

