INSERT INTO sink_t SELECT e, my_sum1(c, 10) as s1, my_sum2(5, c) as s2, my_avg(e, a) as s3 FROM source_t GROUP BY e;
