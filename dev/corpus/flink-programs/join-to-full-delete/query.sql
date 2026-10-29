INSERT INTO sink_t SELECT l.id, r.name, l.`value` FROM left_t l JOIN right_t r ON l.id = r.id;
