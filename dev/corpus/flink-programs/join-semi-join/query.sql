insert into MySink SELECT name FROM EMPLOYEE WHERE deptno IN (SELECT department_num FROM DEPARTMENT);
