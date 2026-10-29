SELECT id, user_id, amount
FROM (
  SELECT *, ROW_NUMBER() OVER (ORDER BY amount DESC, id DESC) AS rn
  FROM events
)
WHERE rn <= 3;
