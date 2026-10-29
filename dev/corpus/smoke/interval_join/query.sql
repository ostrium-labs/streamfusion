SELECT e.id, c.cid, e.user_id, e.ts, c.cts
FROM events AS e, clicks AS c
WHERE e.user_id = c.user_id
  AND c.cts BETWEEN e.ts - INTERVAL '2' SECOND AND e.ts;
