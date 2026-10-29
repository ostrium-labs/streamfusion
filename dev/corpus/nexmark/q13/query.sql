-- -------------------------------------------------------------------------------------------------
-- Query 13: Bounded Side Input Join (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Joins a stream to a bounded side input, modeling basic stream enrichment.
-- -------------------------------------------------------------------------------------------------
-- Vendored from vendor/queries/q13.sql, ADAPTED: upstream joins side_input FOR SYSTEM_TIME AS OF
-- B.p_time (a processing-time lookup join); the filesystem connector is not a lookup source and
-- processing time is not deterministic, so this is a regular inner join on the same key. The
-- side input never changes, so every bid meets the same row it would have looked up.

INSERT INTO nexmark_q13
SELECT
    B.auction,
    B.bidder,
    B.price,
    B.`dateTime`,
    S.`value`
FROM bid AS B
JOIN side_input AS S
ON mod(B.auction, 10000) = S.key;
