INSERT INTO sink_t SELECT
     window_start,
     window_end,
     SUM(price)
 FROM
     TABLE(TUMBLE(TABLE bid_t, DESCRIPTOR(bid_time), INTERVAL '10' SECOND))
 GROUP BY window_start, window_end;
