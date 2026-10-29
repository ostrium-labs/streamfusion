-- Generated from Apache Flink 2.2.1's TableTestProgram "anti-join-on-nested" (Apache-2.0).
CREATE TABLE source_t1 (
  `ext` ROW<`nested` STRING NOT NULL>
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t1',
  'format' = 'json'
);

CREATE TABLE source_t2 (
  `ext` ROW<`nested` ROW<`nested1` ROW<`nested2` STRING NOT NULL>>>
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t2',
  'format' = 'json'
);

CREATE TABLE sink_t (
  output STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

