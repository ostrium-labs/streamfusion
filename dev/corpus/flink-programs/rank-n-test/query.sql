insert into `result1` select * from (select a, b, row_number() over(partition by a order by t asc) as c from MyTable1) where c <= 2;
