-- -------------------------------------------------------------------------------------------------
-- Query 18: Find last bid (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- What's a's last bid for bidder to auction?
-- Illustrates a Deduplicate query.
-- -------------------------------------------------------------------------------------------------
-- Vendored verbatim from vendor/queries/q18.sql.

INSERT INTO nexmark_q18
SELECT auction, bidder, price, channel, url, `dateTime`, extra
 FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY `dateTime` DESC) AS rank_number
       FROM bid)
 WHERE rank_number <= 1;
