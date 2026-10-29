INSERT INTO `sink`
SELECT
    o.user_id,
    o.order_id,
    p.user_id,
    p.payment_id,
    u.user_id,
    u.description
FROM Users u
JOIN `Orders` AS o
  ON  o.user_id = u.user_id
JOIN Payments p
  ON  o.user_id = p.user_id;
