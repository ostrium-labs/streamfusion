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

-- 40 clicks, one every 1.5 s, on the same four users as events.
CREATE TABLE clicks (
  cid BIGINT,
  user_id AS (cid * 3) % 4,
  page AS CONCAT('p', CAST(cid % 6 AS STRING)),
  cts AS TO_TIMESTAMP_LTZ(cid * 1500, 3),
  WATERMARK FOR cts AS cts - INTERVAL '1' SECOND
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '40',
  'fields.cid.kind' = 'sequence',
  'fields.cid.start' = '1',
  'fields.cid.end' = '40'
);
