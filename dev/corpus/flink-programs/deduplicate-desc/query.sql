insert into MySink select order_id, user, product, order_time 
FROM (  SELECT *,    ROW_NUMBER() OVER (PARTITION BY product ORDER BY event_time DESC) AS row_num
  FROM MyTable)WHERE row_num = 1;
