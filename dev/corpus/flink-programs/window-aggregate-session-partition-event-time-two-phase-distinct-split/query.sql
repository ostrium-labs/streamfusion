INSERT INTO window_sink_t SELECT name, window_start, window_end, COUNT(*), SUM(a_int), COUNT(DISTINCT `comment`) FROM TABLE(SESSION(TABLE cdc_window_source_t PARTITION BY name, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
GROUP BY name, window_start, window_end;
