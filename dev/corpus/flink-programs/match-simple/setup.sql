-- Generated from Apache Flink 2.2.1's TableTestProgram "match-simple" (Apache-2.0).
CREATE TABLE MyTable (
  id bigint,
  name varchar,
  proctime as PROCTIME()
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/MyTable',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE MySink (
  a bigint,
  b bigint,
  c bigint
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/MySink',
  'format' = 'debezium-json'
);

