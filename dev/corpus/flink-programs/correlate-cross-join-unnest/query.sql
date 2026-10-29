INSERT INTO sink_t SELECT name, nested FROM source_t CROSS JOIN UNNEST(arr) AS T(nested);
