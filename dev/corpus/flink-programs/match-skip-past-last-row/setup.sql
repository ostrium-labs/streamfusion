-- Generated from Apache Flink 2.2.1's TableTestProgram "match-skip-past-last-row" (Apache-2.0).
CREATE TABLE MyTable (
    vehicle_id bigint,
  engine_temperature int,
  proctime as PROCTIME()
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/MyTable',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE MySink (
    vehicle_id bigint,
  Initial_Temp int,
  Final_Temp int

) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/MySink',
  'format' = 'debezium-json'
);

