-- -------------------------------------------------------------------------------------------------
-- Query 10: Log to File System (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Log all events to file system. Illustrates windows streaming data into partitioned file system.
--
-- Every minute, save all events from the last period into partitioned log files.
-- -------------------------------------------------------------------------------------------------
-- Vendored verbatim from vendor/queries/q10.sql.

INSERT INTO nexmark_q10
SELECT auction, bidder, price, `dateTime`, extra, DATE_FORMAT(`dateTime`, 'yyyy-MM-dd'), DATE_FORMAT(`dateTime`, 'HH:mm')
FROM bid;
