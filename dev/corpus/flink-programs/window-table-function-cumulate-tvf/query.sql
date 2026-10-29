INSERT INTO sink_t SELECT
      bid_time,
     price,
     item,
     window_start,
     window_end,
     window_time
 FROM
     TABLE(CUMULATE(TABLE bid_t, DESCRIPTOR(bid_time), INTERVAL '5' SECOND, INTERVAL '10' SECOND));
