insert into sink_t SELECT `int` FROM source_t order by rowtime, `double`;
