CREATE TABLE sales (
  id INT,
  region STRING,
  price DECIMAL(10, 3),
  sold TIMESTAMP(0)
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/sales.csv',
  'format' = 'csv'
);
