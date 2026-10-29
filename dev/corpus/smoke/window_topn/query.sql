SELECT window_start, window_end, id, amount, rn
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY amount DESC, id) AS rn
  FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '10' SECOND))
)
WHERE rn <= 2;
