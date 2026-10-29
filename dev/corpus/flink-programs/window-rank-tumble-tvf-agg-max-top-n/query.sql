INSERT INTO sink_t SELECT *
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price DESC) as row_num
    FROM (
      SELECT window_start, window_end, supplier_id, SUM(price) as price, COUNT(*) as cnt
      FROM TABLE(TUMBLE(TABLE bid_t, DESCRIPTOR(bid_time), INTERVAL '10' SECOND))
      GROUP BY window_start, window_end, supplier_id
    )
  ) WHERE row_num <= 3;
