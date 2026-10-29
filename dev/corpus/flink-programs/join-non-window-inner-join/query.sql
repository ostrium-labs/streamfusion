insert into MySink SELECT t2.a, t2.c, t1.c
FROM (
 SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T1
) as t1
JOIN (
 SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T2
) as t2
ON t1.a = t2.a AND t1.b > t2.b;
