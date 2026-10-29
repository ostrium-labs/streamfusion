-- Generated from Apache Flink 2.2.1's TableTestProgram "three-way-upsert-preserves-key-with-restore" (Apache-2.0).
CREATE TABLE Users (
  user_id INT NOT NULL,
  shard_id INT NOT NULL,
  description STRING,
  CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id`) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Users',
  'format' = 'debezium-json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE Orders (
  user_id INT NOT NULL,
  order_id BIGINT NOT NULL,
  product STRING,
  CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id`, `order_id`) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Orders',
  'format' = 'debezium-json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE Payments (
  user_id INT NOT NULL,
  payment_id BIGINT NOT NULL,
  product STRING,
  CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id`, `payment_id`) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/Payments',
  'format' = 'debezium-json',
  'source.monitor-interval' = '200ms'
);

CREATE TABLE sink (
  `user_id` INT NOT NULL,
  `order_id` BIGINT NOT NULL,
  `user_id2` INT NOT NULL,
  payment_id BIGINT NOT NULL,
  `user_id3` INT NOT NULL,
  description STRING,
  CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id`, `order_id`, `user_id2`, `payment_id`, `user_id3`) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

