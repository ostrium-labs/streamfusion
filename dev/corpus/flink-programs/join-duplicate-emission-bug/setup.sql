-- Generated from Apache Flink 2.2.1's TableTestProgram "join-duplicate-emission-bug" (Apache-2.0).
CREATE TABLE upsert_table_with_duplicates (
  `execution_plan_id` VARCHAR(2147483647) NOT NULL,
  `workflow_id` VARCHAR(2147483647) NOT NULL,
  `event_section_id` VARCHAR(2147483647) NOT NULL,
  CONSTRAINT `PRIMARY` PRIMARY KEY (`execution_plan_id`, `event_section_id`) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/upsert_table_with_duplicates',
  'format' = 'debezium-json'
);

CREATE TABLE sink (
  event_element_id STRING,
  cnt BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

