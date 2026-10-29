INSERT INTO sink SELECT u.user_id, u.name, o.order_id FROM UsersNullSafe u INNER JOIN OrdersNullSafe o ON u.user_id IS NOT DISTINCT FROM o.user_id;
