INSERT INTO sink SELECT u.user_id, u.name, o.order_id, p.payment_id FROM UsersNulls u LEFT JOIN OrdersNulls o ON u.user_id = o.user_id LEFT JOIN PaymentsNulls p ON u.user_id = p.user_id;
