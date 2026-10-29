-- Generated from Apache Flink 2.2.1's TableTestProgram "expand" (Apache-2.0).
CREATE TABLE MyTable (
  a int,
  b bigint,
  c varchar
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/MyTable',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE MySink (
  b bigint,
  a bigint,
  c varchar,
  primary key (b) not enforced
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/MySink',
  'format' = 'debezium-json'
);

