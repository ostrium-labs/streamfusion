INSERT INTO sink_t SELECT t2.ext.nested.nested1.nested2 FROM source_t2 t2 WHERE NOT EXISTS (SELECT 1 FROM source_t1 t1 WHERE t1.ext.nested = t2.ext.nested.nested1.nested2);
