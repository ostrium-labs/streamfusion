SELECT region, CAST(sold AS DATE) AS sold_day, COUNT(*) AS n, SUM(price) AS revenue, MAX(price) AS top
FROM sales
GROUP BY region, CAST(sold AS DATE)
ORDER BY region, sold_day;
