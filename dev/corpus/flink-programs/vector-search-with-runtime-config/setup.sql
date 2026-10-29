-- Generated from Apache Flink 2.2.1's TableTestProgram "vector-search-with-runtime-config" (Apache-2.0).
CREATE TABLE src_t (
  id BIGINT,
  content STRING,
  vector ARRAY<FLOAT>
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/src_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE async_vector_table (
  label STRING,
  vector ARRAY<FLOAT>
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/async_vector_table',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  id BIGINT,
  content STRING,
  label STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

