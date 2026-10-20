-- MART: revenue by payment mode plus the amount that does not reconcile with the gateway (Q4).
CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
WITH rev AS (
    SELECT payment_mode, count(*) AS delivered_orders, round(sum(order_amount), 2) AS net_revenue
    FROM stg_orders WHERE dq_status = 'PASSED' AND order_status = 'DELIVERED' GROUP BY 1
),
unrec AS (
    SELECT o.payment_mode,
           count(*)                                  AS unreconciled_orders,
           round(sum(abs(r.delta)), 2)               AS unreconciled_amount
    FROM mart_payment_reconciliation r JOIN stg_orders o USING (order_id)
    WHERE r.reason IN ('delivered_without_payment', 'delivered_but_payment_failed', 'amount_mismatch')
    GROUP BY 1
)
SELECT rev.payment_mode, rev.delivered_orders, rev.net_revenue,
       coalesce(unrec.unreconciled_orders, 0) AS unreconciled_orders,
       coalesce(unrec.unreconciled_amount, 0) AS unreconciled_amount,
       round(100.0 * coalesce(unrec.unreconciled_amount, 0) / nullif(rev.net_revenue, 0), 2) AS unreconciled_pct
FROM rev LEFT JOIN unrec USING (payment_mode)
ORDER BY net_revenue DESC;
