insert into `sink_t` select * from (select a, b, row_number() over(partition by b order by c) as c from MyTable) where c = 1;
