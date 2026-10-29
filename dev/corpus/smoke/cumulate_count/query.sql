SELECT user_id, window_start, window_end, COUNT(*) AS n, MAX(amount) AS hi
FROM TABLE(CUMULATE(TABLE events, DESCRIPTOR(ts), INTERVAL '5' SECOND, INTERVAL '20' SECOND))
GROUP BY user_id, window_start, window_end;
