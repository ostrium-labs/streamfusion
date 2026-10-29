INSERT INTO sink SELECT * FROM f(input => TABLE t, scalar => pojoCreator('Bob', 12));
