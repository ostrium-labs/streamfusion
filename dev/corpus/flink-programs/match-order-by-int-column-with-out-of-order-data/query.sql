insert into MySink SELECT * FROM MyEventTimeTable MATCH_RECOGNIZE (
   ORDER BY rowtime, sequence_num
  MEASURES
    FIRST(DOWN.price) as first,
    LAST(DOWN.price) as last,
    UP.price as up
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (DOWN{2,} UP)
  DEFINE
    DOWN AS price < LAST(DOWN.price, 1) OR LAST(DOWN.price, 1) IS NULL,
    UP AS price > LAST(DOWN.price)
) AS T;
