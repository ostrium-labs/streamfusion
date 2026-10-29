-- Generated from Apache Flink 2.2.1's TableTestProgram "temporal-sort-rowtime" (Apache-2.0).
CREATE TABLE source_t (
  ts STRING,
  `int` INT,
  `double` DOUBLE,
  `rowtime` AS TO_TIMESTAMP(`ts`),
  WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  a INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

