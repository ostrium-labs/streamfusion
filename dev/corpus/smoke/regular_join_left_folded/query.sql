SELECT u.uid, u.region, e.id, e.amount
FROM users AS u
LEFT JOIN events AS e ON u.uid = e.user_id AND e.id <= 12;
