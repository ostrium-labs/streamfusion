INSERT INTO `sink`
SELECT
    u.user_id_0,
    o.order_id,
    o.user_id_1,
    p.payment_id,
    p.user_id_2,
    u.name,
    a.location
FROM Users u
JOIN Orders o
  ON  u.user_id_0 = o.user_id_1 AND o.product IS NOT NULL
JOIN Payments p
  ON  u.user_id_0 = p.user_id_2 AND p.price >= 0
JOIN Address a
  ON  u.user_id_0 = a.user_id_3 AND a.location IS NOT NULL;
