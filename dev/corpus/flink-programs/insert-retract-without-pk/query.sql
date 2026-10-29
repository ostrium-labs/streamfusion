INSERT INTO sink_t SELECT name, SUM(score) FROM source_t GROUP BY name;
