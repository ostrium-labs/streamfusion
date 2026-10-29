-- -------------------------------------------------------------------------------------------------
-- Query 12: Processing Time Windows (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many bids does a user make within a fixed processing time limit?
-- Illustrates working in processing time window.
--
-- Group bids by the same user into processing time windows of 10 seconds.
-- Emit the count of bids per window.
-- -------------------------------------------------------------------------------------------------
-- Vendored from vendor/queries/q12.sql, ADAPTED: TUMBLE over bid on the event time `dateTime`
-- instead of over the view B on PROCTIME() (not deterministic). Otherwise verbatim.

INSERT INTO nexmark_q12
SELECT
    bidder,
    count(*) as bid_count,
    window_start AS starttime,
    window_end AS endtime
FROM TABLE(
        TUMBLE(TABLE bid, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))
GROUP BY bidder, window_start, window_end;
