-- Generated from Apache Flink 2.2.1's TableTestProgram "three-way-mixed-changelog-modes" (Apache-2.0).
CREATE TABLE AppendTable (
  id STRING PRIMARY KEY NOT ENFORCED, val STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/AppendTable',
  'format' = 'json'
);

CREATE TABLE RetractTable (
  ref_id STRING, data STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/RetractTable',
  'format' = 'debezium-json'
);

CREATE TABLE UpsertTable (
  key_id STRING PRIMARY KEY NOT ENFORCED, status STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/UpsertTable',
  'format' = 'debezium-json'
);

CREATE TABLE sink (
  id STRING,
  val STRING,
  data STRING,
  status STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

