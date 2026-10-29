insert into MySink SELECT T.aid, T.bid, T.cid
     FROM MyTable MATCH_RECOGNIZE (
             ORDER BY proctime
             MEASURES
             `A"`.id AS aid,
             l.id AS bid,
             C.id AS cid
             PATTERN (`A"` l C)
             DEFINE
                 `A"` AS name = 'a',
                 l AS name = 'b',
                 C AS name = 'c'
     ) AS T;
