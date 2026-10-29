INSERT INTO sink SELECT * FROM f(r => TABLE t, on_time => DESCRIPTOR(ts));
