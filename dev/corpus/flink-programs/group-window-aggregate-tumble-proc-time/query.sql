INSERT INTO sink_t SELECT name, COUNT(*), SUM(a_int), COUNT(DISTINCT `comment`) FROM source_t GROUP BY name, TUMBLE(proctime, INTERVAL '5' SECOND);
