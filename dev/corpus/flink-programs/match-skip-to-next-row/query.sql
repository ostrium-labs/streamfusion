insert into MySink SELECT * FROM
 MyTable
   MATCH_RECOGNIZE(
   PARTITION BY vehicle_id
   ORDER BY `proctime`
   MEASURES 
       FIRST(A.engine_temperature) as Initial_Temp,
       LAST(A.engine_temperature) as Final_Temp
   ONE ROW PER MATCH
   AFTER MATCH SKIP TO NEXT ROW
   PATTERN (A+ B)
   DEFINE
       A as LAST(A.engine_temperature,1) is NULL OR A.engine_temperature > LAST(A.engine_temperature,1),
       B as B.engine_temperature < LAST(A.engine_temperature)
   )MR;
