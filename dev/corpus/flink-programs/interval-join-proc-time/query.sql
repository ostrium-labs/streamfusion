INSERT INTO sink_t SELECT
     o.id AS order_id,
     o.order_ts_str,
     s.shipment_ts_str
 FROM orders_t o
 JOIN shipments_t s ON o.id = s.order_id
 WHERE o.proc_time BETWEEN s.proc_time - INTERVAL '5' SECOND AND s.proc_time + INTERVAL '5' SECOND;
