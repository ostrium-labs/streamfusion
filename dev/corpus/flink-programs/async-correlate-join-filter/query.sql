INSERT INTO sink_t SELECT * FROM (SELECT c, s FROM source_t, LATERAL TABLE(func1(c)) AS T(s)) AS T2 WHERE c LIKE '%hello%' OR c LIKE '%fiz%';
