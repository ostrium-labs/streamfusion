-- 60 events, one a second, four users; every column derives from a datagen sequence.
CREATE TABLE events (
  id BIGINT,
  user_id AS id % 4,
  amount AS CAST(id * 7 % 13 AS DOUBLE) / 4,
  ts AS TO_TIMESTAMP_LTZ(id * 1000 + (id % 3) * 300, 3),
  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '60',
  'fields.id.kind' = 'sequence',
  'fields.id.start' = '1',
  'fields.id.end' = '60'
);
