INSERT INTO sink SELECT * FROM f(scoreTable => TABLE t PARTITION BY name, cityTable => TABLE city PARTITION BY name, on_time => DESCRIPTOR(ts));
