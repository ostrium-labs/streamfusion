INSERT INTO sink_t1_union_all_t2 (SELECT * FROM source_t1 where a >=3) UNION ALL (select * from source_t2 where a <= 3);
