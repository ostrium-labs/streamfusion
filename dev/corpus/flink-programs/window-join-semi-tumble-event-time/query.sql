INSERT INTO sink_t SELECT window_start, window_end, name, id, num, 'NOT_PRESENT', 0
           FROM (
               SELECT * FROM TABLE(TUMBLE(TABLE source_one_t, DESCRIPTOR(row_time), INTERVAL '5' SECOND))
           ) L WHERE EXISTS (
             SELECT * FROM (   
               SELECT * FROM TABLE(TUMBLE(TABLE source_two_t, DESCRIPTOR(row_time), INTERVAL '5' SECOND))
             ) R              WHERE L.name = R.name AND L.window_start = R.window_start AND L.window_end = R.window_end);
