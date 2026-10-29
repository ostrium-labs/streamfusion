-- Generated from Apache Flink 2.2.1's TableTestProgram "constraint-enforcer-error-not-null-nested-rows" (Apache-2.0).
CREATE TABLE source_t (
  a ROW<id BIGINT NOT NULL, ab ROW<     aba BIGINT NOT NULL,     ignore BIGINT> NOT NULL> NOT NULL
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'json'
);

CREATE TABLE sink_t (
  a ROW<id BIGINT NOT NULL, ab ROW<     aba BIGINT NOT NULL,     ignore BIGINT> NOT NULL> NOT NULL
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

