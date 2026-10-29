INSERT INTO sink SELECT * FROM f(u => TABLE t PARTITION BY name, i => 1);
