INSERT INTO sink SELECT `name`, `out` FROM f(scoreTable => TABLE scores PARTITION BY name, cityTable => TABLE city PARTITION BY name);
