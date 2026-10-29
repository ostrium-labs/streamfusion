insert into MySink select a, count(distinct b) as b, first_value(c) c from MyTable group by a;
