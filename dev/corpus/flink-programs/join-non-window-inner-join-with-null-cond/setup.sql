-- Generated from Apache Flink 2.2.1's TableTestProgram "join-non-window-inner-join-with-null-cond" (Apache-2.0).
CREATE TABLE T1 (
  a int,
  b bigint,
  c varchar
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/T1',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE T2 (
  a int,
  b bigint,
  c varchar
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/T2',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE MySink (
  a int,
  c1 varchar,
  c2 varchar
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/MySink',
  'format' = 'debezium-json'
);

