SELECT user_id, COUNT(*) AS n, SUM(amount) AS total, MIN(amount) AS lo, AVG(amount) AS mean
FROM events
WHERE id <= 20
GROUP BY user_id;
