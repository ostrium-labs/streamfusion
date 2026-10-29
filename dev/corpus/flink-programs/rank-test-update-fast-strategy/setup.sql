-- Generated from Apache Flink 2.2.1's TableTestProgram "rank-test-update-fast-strategy" (Apache-2.0).
CREATE TABLE MyTable (
  a INT primary key not enforced,
  b VARCHAR,
  c INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/MyTable',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  a INT NOT NULL,
  b STRING,
  count_c BIGINT NOT NULL,
  row_num BIGINT NOT NULL
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

