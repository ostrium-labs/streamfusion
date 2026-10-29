INSERT INTO sink SELECT * FROM f(columnList1 => NULL, columnList3 => DESCRIPTOR(a, b, c));
