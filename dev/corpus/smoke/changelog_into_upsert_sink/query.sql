INSERT INTO totals
SELECT user_id, COUNT(*), SUM(amount) FROM events GROUP BY user_id;
