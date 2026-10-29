INSERT INTO sink_t SELECT b FROM source_t1  WHERE (CASE WHEN a IN (SELECT 1 FROM source_t3) THEN 1 ELSE 2 END)  IN (SELECT d FROM source_t2 WHERE source_t1.c = source_t2.f);
