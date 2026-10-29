-- Generated from Apache Flink 2.2.1's TableTestProgram "three-way-join-with-time-attributes-in-join-conditions" (Apache-2.0).
CREATE TABLE auctions (
  id BIGINT PRIMARY KEY NOT ENFORCED,
  category STRING,
  auctionTimestamp STRING,
  expiresTimestamp STRING,
  auctionDateTime AS TO_TIMESTAMP(auctionTimestamp),
  expires AS TO_TIMESTAMP(expiresTimestamp),
  WATERMARK FOR auctionDateTime AS auctionDateTime - INTERVAL '1' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/auctions',
  'format' = 'json'
);

CREATE TABLE bids (
  auction BIGINT,
  price DOUBLE,
  bidTimestamp STRING,
  bidDateTime AS TO_TIMESTAMP(bidTimestamp),
  WATERMARK FOR bidDateTime AS bidDateTime - INTERVAL '1' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '${CASE_DIR}/bids',
  'format' = 'json'
);

CREATE TABLE sink (
  category STRING,
  avg_final DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = '${SINK_DIR}/sink',
  'format' = 'debezium-json'
);

