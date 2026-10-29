INSERT INTO window_sink_t SELECT name, window_start, window_end, COUNT(*), SUM(a_int), COUNT(DISTINCT `comment`) FROM TABLE(CUMULATE(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '15' SECOND))
GROUP BY name, window_start, window_end;
