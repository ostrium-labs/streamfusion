INSERT INTO sink_t SELECT id, content, label FROM src_t, LATERAL TABLE(
VECTOR_SEARCH(TABLE sync_vector_table, DESCRIPTOR(vector), src_t.vector, 2));
