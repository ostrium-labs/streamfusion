INSERT INTO sink_t SELECT
     window_start,
     window_end,
     SUM(price)
 FROM
     TABLE(HOP(TABLE bid_t, DESCRIPTOR(bid_time), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
 GROUP BY window_start, window_end;
