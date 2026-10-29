INSERT INTO sink SELECT u.name, o.order_id, o.rowtime, p.price FROM UsersWithProctime u INNER JOIN OrdersWithRowtime o ON u.user_id_0 = o.user_id_1 INNER JOIN Payments p ON u.user_id_0 = p.user_id_2;
