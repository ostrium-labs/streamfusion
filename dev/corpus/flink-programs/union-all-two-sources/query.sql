INSERT INTO sink_t1_union_all_t2 SELECT * FROM (SELECT a, b FROM source_t1) UNION ALL (SELECT d, e FROM source_t2);
