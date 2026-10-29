insert into MySink SELECT * FROM MyTable MATCH_RECOGNIZE (
  PARTITION BY symbol
  ORDER BY proctime
  MEASURES
    FIRST(DOWN.price) as first,
    LAST(DOWN.price) as last,
    FIRST(DOWN.price, 5) as nullPrice
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (DOWN{2,} UP)
  DEFINE
    DOWN AS price < LAST(DOWN.price, 1) OR LAST(DOWN.price, 1) IS NULL,
    UP AS price > LAST(DOWN.price)
) AS T;
