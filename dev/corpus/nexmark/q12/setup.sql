-- Nexmark's person, auction and bid streams as three filesystem tables over the
-- shared dataset in dev/corpus/nexmark/data (see ../README.md for how it was
-- generated). Upstream reads one 'nexmark' connector table (ddl_gen.sql) through
-- three views (ddl_views.sql); the columns, their types and the event-time
-- watermark on `dateTime` (4 s bounded out-of-orderness) are upstream's.
CREATE TABLE person (
    id  BIGINT,
    name  VARCHAR,
    emailAddress  VARCHAR,
    creditCard  VARCHAR,
    city  VARCHAR,
    state  VARCHAR,
    `dateTime`  TIMESTAMP(3),
    extra  VARCHAR,
    WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND
) WITH (
    'connector' = 'filesystem',
    'path' = '${CASE_DIR}/person',
    'format' = 'json'
);

CREATE TABLE auction (
    id  BIGINT,
    itemName  VARCHAR,
    description  VARCHAR,
    initialBid  BIGINT,
    reserve  BIGINT,
    `dateTime`  TIMESTAMP(3),
    expires  TIMESTAMP(3),
    seller  BIGINT,
    category  BIGINT,
    extra  VARCHAR,
    WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND
) WITH (
    'connector' = 'filesystem',
    'path' = '${CASE_DIR}/auction',
    'format' = 'json'
);

CREATE TABLE bid (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    `dateTime`  TIMESTAMP(3),
    extra  VARCHAR,
    WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND
) WITH (
    'connector' = 'filesystem',
    'path' = '${CASE_DIR}/bid',
    'format' = 'json'
);

-- The sink: upstream's columns, written as debezium-json under the run's sink directory
-- (upstream: 'blackhole').
CREATE TABLE nexmark_q12 (
  bidder BIGINT,
  bid_count BIGINT,
  starttime TIMESTAMP(3),
  endtime TIMESTAMP(3)
) WITH (
    'connector' = 'filesystem',
    'path' = '${SINK_DIR}/nexmark_q12',
    'format' = 'debezium-json'
);

-- Upstream q12.sql also defines CREATE VIEW B AS SELECT *, PROCTIME() as p_time FROM bid,
-- and windows on p_time. Processing time makes the result depend on the wall clock, so
-- this case windows on the event time `dateTime` instead (see query.sql).
