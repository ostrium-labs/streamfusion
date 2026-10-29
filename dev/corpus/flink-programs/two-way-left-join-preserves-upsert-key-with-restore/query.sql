INSERT INTO `sink`
SELECT
    o.user_id,
    o.order_id,
    o.product,
    u.shard_id
FROM Orders o
LEFT JOIN Users u
  ON  u.user_id = o.user_id;
