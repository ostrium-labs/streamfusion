-- Generated from Apache Flink 2.2.1's TableTestProgram "two-way-inner-join-with-where-in" (Apache-2.0).
CREATE TABLE Records (
  `record_id` STRING PRIMARY KEY NOT ENFORCED,
  `user_id` INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Records',
  'format' = 'debezium-json'
);

CREATE TABLE Users (
  `user_id` INT PRIMARY KEY NOT ENFORCED,
  `id` STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Users',
  'format' = 'debezium-json'
);

CREATE TABLE sink (
  record_id STRING,
  `user_id` INT,
  id STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

