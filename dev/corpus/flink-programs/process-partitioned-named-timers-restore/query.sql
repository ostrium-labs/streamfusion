INSERT INTO sink SELECT * FROM f(r => TABLE t PARTITION BY name, on_time => DESCRIPTOR(ts));
