SELECT window_start, window_end, SUM(amount) AS total, COUNT(DISTINCT user_id) AS users
FROM TABLE(HOP(TABLE events, DESCRIPTOR(ts), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
GROUP BY window_start, window_end;
