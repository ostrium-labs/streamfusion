-- Generated from Apache Flink 2.2.1's TableTestProgram "join-to-full-delete" (Apache-2.0).
CREATE TABLE left_t (
  id INT PRIMARY KEY NOT ENFORCED,
  `value` INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/left_t',
  'format' = 'debezium-json'
);

CREATE TABLE right_t (
  id INT PRIMARY KEY NOT ENFORCED,
  name STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/right_t',
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

