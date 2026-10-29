SELECT user_id, window_start, window_end, window_time, COUNT(*) AS n, SUM(amount) AS total, AVG(amount) AS mean
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '10' SECOND))
GROUP BY user_id, window_start, window_end, window_time;
