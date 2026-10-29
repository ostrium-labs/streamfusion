-- -------------------------------------------------------------------------------------------------
-- Query 14: Calculation (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Convert bid timestamp into types and find bids with specific price.
-- Illustrates duplicate expressions and usage of user-defined-functions.
-- -------------------------------------------------------------------------------------------------
-- Vendored from vendor/queries/q14.sql, ADAPTED: count_char(extra, 'c') (upstream's CountChar UDF:
-- the number of bytes of extra equal to 'c', 0 for NULL) becomes
-- CAST(CHAR_LENGTH(extra) - CHAR_LENGTH(REPLACE(extra, 'c', '')) AS BIGINT), the same for the
-- ASCII, never-NULL extra the generator writes. Otherwise verbatim.

INSERT INTO nexmark_q14
SELECT 
    auction,
    bidder,
    0.908 * price as price,
    CASE
        WHEN HOUR(`dateTime`) >= 8 AND HOUR(`dateTime`) <= 18 THEN 'dayTime'
        WHEN HOUR(`dateTime`) <= 6 OR HOUR(`dateTime`) >= 20 THEN 'nightTime'
        ELSE 'otherTime'
    END AS bidTimeType,
    `dateTime`,
    extra,
    CAST(CHAR_LENGTH(extra) - CHAR_LENGTH(REPLACE(extra, 'c', '')) AS BIGINT) AS c_counts
FROM bid
WHERE 0.908 * price > 1000000 AND 0.908 * price < 50000000;
