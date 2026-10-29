-- Generated from Apache Flink 2.2.1's TableTestProgram "group-window-aggregate-hop-proc-time" (Apache-2.0).
CREATE TABLE source_t (
  ts STRING,
  a_int INT,
  b_double DOUBLE,
  c_float FLOAT,
  d_bigdec DECIMAL(10, 2),
  `comment` STRING,
  name STRING,
  `rowtime` AS TO_TIMESTAMP(`ts`),
  `proctime` AS PROCTIME(),
  WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/source_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  name STRING,
  cnt BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

