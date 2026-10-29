INSERT INTO sink SELECT a.id, a.val, r.data, u.status FROM AppendTable a LEFT JOIN RetractTable r ON a.id = r.ref_id LEFT JOIN UpsertTable u ON a.id = u.key_id;
