INSERT INTO sink SELECT u.user_id, u.name, o.order_id, p.payment_id FROM Users u INNER JOIN Orders o ON u.user_id = o.user_id INNER JOIN Payments p ON u.user_id = p.user_id;
