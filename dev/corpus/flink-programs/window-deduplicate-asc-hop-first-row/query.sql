INSERT INTO sink_t SELECT *
  FROM (
    SELECT
         bid_time,
         price,
         item,
         supplier_id,
         window_start,
         window_end, 
         ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY bid_time ASC) AS row_num
    FROM TABLE(HOP(TABLE bid_t, DESCRIPTOR(bid_time), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
  ) WHERE row_num <= 1;
