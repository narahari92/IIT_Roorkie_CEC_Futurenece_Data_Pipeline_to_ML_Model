-- MART: one row per store per day. Answers Q1 (net revenue by zone/store) and feeds Q3.
-- Only dq_status = 'PASSED' rows; DELIVERED orders only count as revenue.
-- Bonus: store-capacity check (daily orders vs capacity_orders_per_day).
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT o.order_date,
       s.zone, o.store_id, s.store_name,
       count(*)                                                   AS orders,
       count(*) FILTER (WHERE o.order_status = 'DELIVERED')       AS delivered_orders,
       count(*) FILTER (WHERE o.order_status = 'CANCELLED')       AS cancelled_orders,
       count(*) FILTER (WHERE o.order_status = 'RETURNED')        AS returned_orders,
       round(sum(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END), 2)                    AS net_revenue,
       round(sum(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount - o.quantity * p.cost_price ELSE 0 END), 2) AS gross_margin,
       round(avg(o.delivery_minutes) FILTER (WHERE o.order_status = 'DELIVERED'), 2)                           AS avg_delivery_minutes,
       count(*) FILTER (WHERE o.order_status = 'DELIVERED' AND o.delivery_minutes <= 15)                        AS delivered_within_15,
       round(100.0 * count(*) FILTER (WHERE o.order_status = 'DELIVERED' AND o.delivery_minutes <= 15)
             / nullif(count(*) FILTER (WHERE o.order_status = 'DELIVERED'), 0), 2)                             AS pct_within_15,
       s.capacity_orders_per_day,
       count(*) > coalesce(any_value(s.capacity_orders_per_day), 0) AS over_capacity
FROM stg_orders o
JOIN stg_stores   s ON s.store_id = o.store_id
JOIN stg_products p ON p.product_id = o.product_id
WHERE o.dq_status = 'PASSED'
GROUP BY ALL
ORDER BY o.order_date, s.zone, o.store_id;
