SELECT id, user_id, amount, ts
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts ASC) AS rn
  FROM events
)
WHERE rn = 1;
