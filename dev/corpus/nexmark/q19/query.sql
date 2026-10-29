-- -------------------------------------------------------------------------------------------------
-- Query 19: Auction TOP-10 Price (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- What's the top price 10 bids of an auction?
-- Illustrates a TOP-N query.
-- -------------------------------------------------------------------------------------------------
-- Vendored verbatim from vendor/queries/q19.sql.

INSERT INTO nexmark_q19
SELECT * FROM
(SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid)
WHERE rank_number <= 10;
