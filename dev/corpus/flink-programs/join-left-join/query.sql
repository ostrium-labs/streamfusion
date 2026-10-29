insert into MySink SELECT name, department_name FROM EMPLOYEE LEFT JOIN DEPARTMENT ON deptno = department_num;
