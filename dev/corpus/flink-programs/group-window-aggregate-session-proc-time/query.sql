INSERT INTO sink_t SELECT name, COUNT(*) FROM source_t GROUP BY name, SESSION(proctime, INTERVAL '3' SECOND);
