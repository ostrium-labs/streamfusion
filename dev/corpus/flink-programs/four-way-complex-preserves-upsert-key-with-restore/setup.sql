-- Generated from Apache Flink 2.2.1's TableTestProgram "four-way-complex-preserves-upsert-key-with-restore" (Apache-2.0).
CREATE TABLE Users (
  user_id_0 STRING PRIMARY KEY NOT ENFORCED,
  name STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Users',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE Orders (
  order_id STRING NOT NULL,
  user_id_1 STRING NOT NULL,
  product STRING,
  CONSTRAINT `PRIMARY` PRIMARY KEY (`order_id`, `user_id_1`) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Orders',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE Payments (
  payment_id STRING NOT NULL,
  user_id_2 STRING NOT NULL,
  price INT,
  CONSTRAINT `PRIMARY` PRIMARY KEY (`payment_id`, `user_id_2`) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Payments',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE Address (
  user_id_3 STRING NOT NULL,
  location STRING,
  CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id_3`) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Address',
  'format' = 'json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink (
  user_id_0 STRING NOT NULL,
  order_id STRING NOT NULL,
  user_id_1 STRING NOT NULL,
  payment_id STRING NOT NULL,
  user_id_2 STRING NOT NULL,
  name STRING,
  location STRING,
  CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id_0`, `order_id`, `user_id_1`, `payment_id`, `user_id_2`) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

