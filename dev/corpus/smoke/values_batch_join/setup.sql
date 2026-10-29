CREATE TEMPORARY VIEW orders (oid, cust, qty, price) AS
VALUES (1, 'ann', 2, 9.99), (2, 'bob', 1, 120.00), (3, 'ann', 5, 0.50), (4, 'cyd', 3, 33.33), (5, 'bob', 7, 1.25);
CREATE TEMPORARY VIEW customers (cust, tier) AS
VALUES ('ann', 'gold'), ('bob', 'silver'), ('dan', 'gold');
