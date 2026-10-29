INSERT INTO sink_t SELECT id, name, `value` + 2 FROM source_t;
