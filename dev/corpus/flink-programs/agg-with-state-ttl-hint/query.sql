INSERT INTO sink_t SELECT /*+ STATE_TTL('source_t' = '1s') */b, COUNT(*) AS cnt, AVG(a) FILTER (WHERE a > 1) AS avg_a, MIN(c) AS min_c FROM source_t GROUP BY b;
