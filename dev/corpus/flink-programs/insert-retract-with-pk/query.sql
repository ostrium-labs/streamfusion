INSERT INTO sink_t SELECT UPPER(name), SUM(score) FROM source_t GROUP BY name;
