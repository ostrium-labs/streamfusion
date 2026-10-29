INSERT INTO sink_t SELECT e, my_avg(e, a) as s1, my_concat(d) as c1 FROM source_t GROUP BY e;
