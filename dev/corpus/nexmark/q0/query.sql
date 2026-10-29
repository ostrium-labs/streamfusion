-- -------------------------------------------------------------------------------------------------
-- Query 0: Pass through (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- This measures the monitoring overhead of the Flink SQL implementation including the source generator.
-- Using `bid` events here, as they are most numerous with default configuration.
-- -------------------------------------------------------------------------------------------------
-- Vendored verbatim from vendor/queries/q0.sql.

INSERT INTO nexmark_q0
SELECT auction, bidder, price, `dateTime`, extra FROM bid;
