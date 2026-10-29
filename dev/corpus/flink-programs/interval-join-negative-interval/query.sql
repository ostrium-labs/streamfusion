INSERT INTO sink_t SELECT
     o.id AS order_id,
     o.order_ts_str,
     s.shipment_ts_str
 FROM orders_t o LEFT OUTER JOIN shipments_t s
 ON o.id = s.order_id
 AND o.order_ts BETWEEN s.shipment_ts + INTERVAL '10' SECOND AND s.shipment_ts + INTERVAL '5' SECOND;
