SELECT id, user_id, amount
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts DESC) AS rn
  FROM events
)
WHERE rn = 1;
