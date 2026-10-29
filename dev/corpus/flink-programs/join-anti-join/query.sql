insert into MySink SELECT name FROM EMPLOYEE WHERE deptno NOT IN (SELECT department_num FROM DEPARTMENT);
