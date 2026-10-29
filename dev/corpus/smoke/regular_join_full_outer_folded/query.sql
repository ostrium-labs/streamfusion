SELECT COALESCE(a.user_id, b.user_id) AS user_id, a.total, b.pages
FROM (SELECT user_id, SUM(amount) AS total FROM events WHERE id % 5 <> 0 GROUP BY user_id) AS a
FULL OUTER JOIN (SELECT user_id + 1 AS user_id, COUNT(DISTINCT page) AS pages FROM clicks GROUP BY user_id + 1) AS b
ON a.user_id = b.user_id;
