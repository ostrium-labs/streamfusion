insert into MySink SELECT name, department_name FROM EMPLOYEE FULL OUTER JOIN DEPARTMENT ON deptno = department_num;
