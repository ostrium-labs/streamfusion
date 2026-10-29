-- Generated from Apache Flink 2.2.1's TableTestProgram "rank-n-test" (Apache-2.0).
CREATE TABLE MyTable1 (
  a STRING,
  b INT,
  c INT,
  t as proctime()
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/MyTable1',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE result1 (
  a varchar,
  b int,
  c bigint
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/result1',
  'format' = 'debezium-json'
);

