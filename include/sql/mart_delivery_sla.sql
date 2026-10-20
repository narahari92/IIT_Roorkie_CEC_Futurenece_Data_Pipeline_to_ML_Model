-- MART: delivery SLA per store (Q3): average delivery time and % delivered within 15 minutes.
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT s.zone, o.store_id, s.store_name,
       count(*)                                           AS delivered_orders,
       round(avg(o.delivery_minutes), 2)                  AS avg_delivery_minutes,
       min(o.delivery_minutes)                            AS min_delivery_minutes,
       max(o.delivery_minutes)                            AS max_delivery_minutes,
       count(*) FILTER (WHERE o.delivery_minutes <= 15)   AS within_15,
       round(100.0 * count(*) FILTER (WHERE o.delivery_minutes <= 15) / count(*), 2) AS pct_within_15
FROM stg_orders o JOIN stg_stores s ON s.store_id = o.store_id
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY pct_within_15 DESC;
