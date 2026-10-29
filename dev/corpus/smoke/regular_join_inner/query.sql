SELECT e.id, e.user_id, u.region, e.amount
FROM events AS e
JOIN users AS u ON e.user_id = u.uid;
