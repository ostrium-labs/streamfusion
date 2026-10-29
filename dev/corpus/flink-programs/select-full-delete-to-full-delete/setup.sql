-- Generated from Apache Flink 2.2.1's TableTestProgram "select-full-delete-to-full-delete" (Apache-2.0).
CREATE TABLE source_t (
  id INT PRIMARY KEY NOT ENFORCED,
  name STRING,
  `value` INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'debezium-json'
);

CREATE TABLE sink_t (
  id INT PRIMARY KEY NOT ENFORCED,
  name STRING,
  `value` INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

