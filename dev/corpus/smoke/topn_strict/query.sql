SELECT user_id, id, amount, rn
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC, id) AS rn
  FROM events
)
WHERE rn <= 2;
