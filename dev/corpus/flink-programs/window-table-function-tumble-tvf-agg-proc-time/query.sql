INSERT INTO sink_t SELECT
     SUM(price)
 FROM
     TABLE(TUMBLE(TABLE bid_t, DESCRIPTOR(proc_time), INTERVAL '10' SECOND))
 GROUP BY window_start, window_end;
