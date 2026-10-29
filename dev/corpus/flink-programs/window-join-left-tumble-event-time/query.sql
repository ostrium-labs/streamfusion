INSERT INTO sink_t SELECT
L.window_start AS window_start,
L.window_end AS window_end,
L.name AS name,
L.id AS L_id,
L.num AS L_num,
R.id AS R_id,
R.num AS R_num
FROM
(
    SELECT * FROM TABLE(TUMBLE(TABLE source_one_t, DESCRIPTOR(row_time), INTERVAL '5' SECOND))
) L
LEFT JOIN
(
    SELECT * FROM TABLE(TUMBLE(TABLE source_two_t, DESCRIPTOR(row_time), INTERVAL '5' SECOND))
) R
ON L.name = R.name
AND L.window_start = R.window_start
AND L.window_end = R.window_end;
