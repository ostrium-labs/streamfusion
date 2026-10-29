INSERT INTO sink_t SELECT
     o.id AS order_id,
     o.order_ts_str,
     s.shipment_ts_str
 FROM orders_t o
 JOIN shipments_t s ON o.id = s.order_id
 WHERE o.order_ts BETWEEN s.shipment_ts - INTERVAL '5' SECOND AND s.shipment_ts + INTERVAL '5' SECOND;
