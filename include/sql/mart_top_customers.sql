-- MART: top 10 customers by delivered spend with loyalty tier (Q5). PII already masked in staging.
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT c.customer_id, c.full_name, c.email_masked, c.phone_masked, c.loyalty_tier, c.area,
       count(*)                           AS delivered_orders,
       round(sum(o.order_amount), 2)      AS total_spend,
       round(avg(o.order_amount), 2)      AS avg_order_value,
       min(o.order_date)                  AS first_order,
       max(o.order_date)                  AS last_order
FROM stg_orders o JOIN stg_customers c ON c.customer_id = o.customer_id
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY total_spend DESC
LIMIT 10;
