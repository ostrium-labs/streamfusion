-- Generated from Apache Flink 2.2.1's TableTestProgram "lookup-join-with-try-resolve" (Apache-2.0).
CREATE TABLE customers_t (
  id INT PRIMARY KEY NOT ENFORCED,
  name STRING,
  age INT,
  city STRING,
  state STRING,
  zipcode INT
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/customers_t',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE orders_cdc_t (
  order_id INT,
  customer_id INT,
  total DOUBLE,
  order_time STRING,
  proc_time AS PROCTIME(),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/orders_cdc_t',
  'format' = 'debezium-json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink_t (
  order_id INT,
  total DOUBLE,
  id INT,
  name STRING,
  city STRING,
  state STRING,
  zipcode INT,
  PRIMARY KEY (order_id, city) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink_t',
  'format' = 'debezium-json'
);

