INSERT INTO sink_t SELECT i, s, ts, CURRENT_WATERMARK(ts) as w FROM source_t;
