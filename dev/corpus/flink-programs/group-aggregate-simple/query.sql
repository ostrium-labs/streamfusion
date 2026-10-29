INSERT INTO sink_t SELECT b, COUNT(*) AS cnt, AVG(a) FILTER (WHERE a > 1) AS avg_a, MIN(c) AS min_c FROM source_t GROUP BY b;
