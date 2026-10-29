INSERT INTO sink_t SELECT
         b,
         SUM(b1) AS sum_b,
         COUNT(DISTINCT b1) AS cnt_distinct_b,
         COUNT(1) AS cnt_1
     FROM
         (
             SELECT
                     a,
                     COUNT(b) AS b,
                     MAX(b) AS b1
             FROM source_t GROUP BY a
         )
     GROUP BY b;
