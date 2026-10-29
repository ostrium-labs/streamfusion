INSERT INTO sink_t SELECT id, content, label FROM src_t, LATERAL TABLE(
VECTOR_SEARCH(
  SEARCH_TABLE => TABLE async_vector_table,
  COLUMN_TO_SEARCH => DESCRIPTOR(vector),
  COLUMN_TO_QUERY => src_t.vector,
  TOP_K => 2,
  CONFIG => MAP['async', 'false']
));
