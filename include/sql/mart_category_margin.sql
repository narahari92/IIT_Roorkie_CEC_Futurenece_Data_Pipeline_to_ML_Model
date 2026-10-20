-- MART: revenue and gross margin by product category (Q2). margin = revenue − quantity × cost_price.
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT p.category,
       count(*)                                        AS delivered_orders,
       sum(o.quantity)                                 AS units,
       round(sum(o.order_amount), 2)                   AS revenue,
       round(sum(o.quantity * p.cost_price), 2)        AS cogs,
       round(sum(o.order_amount - o.quantity * p.cost_price), 2) AS gross_margin,
       round(100 * sum(o.order_amount - o.quantity * p.cost_price) / nullif(sum(o.order_amount), 0), 2) AS margin_pct
FROM stg_orders o JOIN stg_products p ON p.product_id = o.product_id
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY revenue DESC;
