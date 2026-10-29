-- Generated from Apache Flink 2.2.1's TableTestProgram "join-inner-join-with-equal-pk" (Apache-2.0).
CREATE TABLE EMPLOYEE (
  deptno int,
  salary bigint,
  name varchar
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/EMPLOYEE',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE DEPARTMENT (
  department_num int,
  b2 bigint,
  b3 int,
  department_name varchar
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/DEPARTMENT',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE MySink (
  deptno int,
  department_num int
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/MySink',
  'format' = 'debezium-json'
);

