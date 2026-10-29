INSERT INTO sink_t
     SELECT
         b,
         COUNT(DISTINCT a) AS a
     FROM source_t
     GROUP BY b;
