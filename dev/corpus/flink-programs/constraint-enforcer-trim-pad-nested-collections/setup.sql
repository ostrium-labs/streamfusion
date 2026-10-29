-- Generated from Apache Flink 2.2.1's TableTestProgram "constraint-enforcer-trim-pad-nested-collections" (Apache-2.0).
CREATE TABLE source_t (
  a ROW<id BIGINT NOT NULL, ab ARRAY< ROW<     c CHAR(2) NOT NULL,     vc VARCHAR(2) NOT NULL,     b BINARY(2) NOT NULL,     vb VARBINARY(2) NOT NULL > NOT NULL> NOT NULL> NOT NULL
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'json'
);

CREATE TABLE sink_t (
  a ROW<id BIGINT NOT NULL, ab ARRAY< ROW<     c CHAR(2) NOT NULL,     vc VARCHAR(2) NOT NULL,     b BINARY(2) NOT NULL,     vb VARBINARY(2) NOT NULL > NOT NULL> NOT NULL> NOT NULL
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

