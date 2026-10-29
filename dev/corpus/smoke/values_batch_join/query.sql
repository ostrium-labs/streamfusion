SELECT o.oid, UPPER(o.cust) AS cust, c.tier, o.qty * o.price AS amount,
       CASE WHEN o.qty * o.price > 20 THEN 'big' ELSE 'small' END AS size
FROM orders AS o LEFT JOIN customers AS c ON o.cust = c.cust
ORDER BY o.oid;
