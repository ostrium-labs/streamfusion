-- Generated from Apache Flink 2.2.1's TableTestProgram "retract-source-with-key-filter" (Apache-2.0).
CREATE TABLE source_t (
  a STRING,
  b INT,
  c STRING,
  PRIMARY KEY(a) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'debezium-json'
);

CREATE TABLE sink_t (
  a STRING,
  b INT,
  c STRING,
  PRIMARY KEY(a) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

