INSERT INTO sink_t(bid_time, supplier_id) SELECT bid_time, supplier_id
  FROM (
    SELECT
         bid_time,
         supplier_id,
         ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price ASC) AS row_num
    FROM TABLE(HOP(
      DATA => TABLE bid_t,
      TIMECOL => DESCRIPTOR(`bid_time`),
      SLIDE => INTERVAL '5' SECOND,
      SIZE => INTERVAL '10' SECOND))
  ) WHERE row_num <= 3;
