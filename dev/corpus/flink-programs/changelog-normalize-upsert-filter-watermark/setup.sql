-- Generated from Apache Flink 2.2.1's TableTestProgram "changelog-normalize-upsert-filter-watermark" (Apache-2.0).
CREATE TABLE source_t (
  a VARCHAR,
  b INT NOT NULL,
  c VARCHAR,
  d TIMESTAMP_LTZ(3),
  WATERMARK FOR d AS d,
  PRIMARY KEY(a) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'debezium-json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  a VARCHAR,
  b INT,
  c VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

