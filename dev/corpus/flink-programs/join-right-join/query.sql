insert into MySink SELECT name, department_name FROM EMPLOYEE RIGHT OUTER JOIN DEPARTMENT ON deptno = department_num;
