SELECT e.id, e.user_id, c.cid, c.page
FROM events AS e
LEFT JOIN clicks AS c
  ON e.user_id = c.user_id
 AND c.cts BETWEEN e.ts - INTERVAL '1' SECOND AND e.ts + INTERVAL '1' SECOND;
