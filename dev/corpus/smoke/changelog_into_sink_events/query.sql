INSERT INTO totals
SELECT user_id, COUNT(*), SUM(amount) FROM events WHERE id <= 16 GROUP BY user_id;
