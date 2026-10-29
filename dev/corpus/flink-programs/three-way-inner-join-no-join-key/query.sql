INSERT INTO sink SELECT u.user_id, u.name, o.order_id, p.payment_id FROM Users u LEFT JOIN Orders o ON TRUE INNER JOIN Payments p ON TRUE;
