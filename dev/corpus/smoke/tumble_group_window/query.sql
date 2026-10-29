SELECT user_id,
       TUMBLE_START(ts, INTERVAL '20' SECOND) AS ws,
       TUMBLE_END(ts, INTERVAL '20' SECOND) AS we,
       COUNT(*) AS n
FROM events
GROUP BY user_id, TUMBLE(ts, INTERVAL '20' SECOND);
