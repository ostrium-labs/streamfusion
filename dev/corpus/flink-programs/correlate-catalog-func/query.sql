INSERT INTO sink_t SELECT c, s FROM source_t, LATERAL TABLE(func1(c, '$')) AS T(s);
