INSERT INTO sink_t SELECT a, udf1(udf1(udf1(a))) FROM source_t;
