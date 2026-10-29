SELECT e.user_id, e.window_start, e.window_end, e.n, c.clicks
FROM (
  SELECT user_id, window_start, window_end, COUNT(*) AS n
  FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '10' SECOND))
  GROUP BY user_id, window_start, window_end
) e
JOIN (
  SELECT user_id, window_start, window_end, COUNT(*) AS clicks
  FROM TABLE(TUMBLE(TABLE clicks, DESCRIPTOR(cts), INTERVAL '10' SECOND))
  GROUP BY user_id, window_start, window_end
) c
ON e.user_id = c.user_id AND e.window_start = c.window_start AND e.window_end = c.window_end;
