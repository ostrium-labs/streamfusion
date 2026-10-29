INSERT INTO sink_t SELECT a FROM source_t WHERE udf1(a) > 6;
