INSERT INTO sink_t SELECT a, udf1(a) FROM source_t;
