INSERT INTO sink_t SELECT *
  FROM (
    SELECT
         window_start,
         window_end, 
         bid_time,
         supplier_id,
         price,
         item,
         ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price ASC) AS row_num
    FROM TABLE(CUMULATE(TABLE bid_t, DESCRIPTOR(bid_time), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
  ) WHERE row_num <= 3;
