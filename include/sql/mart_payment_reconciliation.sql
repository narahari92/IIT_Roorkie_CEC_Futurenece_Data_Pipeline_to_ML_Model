-- MART: every order/payment mismatch with a reason (Q4). Same reason codes as the ETL table.
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH o AS (SELECT order_id, order_status, order_amount, payment_mode FROM stg_orders   WHERE dq_status = 'PASSED'),
     p AS (SELECT payment_id, order_id, payment_status, amount_paid, payment_mode FROM stg_payments WHERE dq_status = 'PASSED'),
     rejected_orders AS (SELECT DISTINCT row_key AS order_id FROM stg_rejections WHERE entity = 'orders'
                         UNION SELECT order_id FROM stg_orders WHERE dq_status = 'FAILED'),
     j AS (
        SELECT coalesce(o.order_id, p.order_id) AS order_id, p.payment_id, o.order_status, o.order_amount,
               p.payment_status, p.amount_paid, o.payment_mode AS order_mode, p.payment_mode AS pay_mode,
               o.order_id IS NOT NULL AS has_order, p.payment_id IS NOT NULL AS has_payment
        FROM o FULL OUTER JOIN p ON o.order_id = p.order_id
     )
SELECT order_id, payment_id, order_status, order_amount, payment_status, amount_paid,
       coalesce(amount_paid, 0) - coalesce(order_amount, 0) AS delta,
       CASE
         WHEN NOT has_order AND order_id IN (SELECT order_id FROM rejected_orders) THEN 'payment_for_quarantined_order'
         WHEN NOT has_order                                                       THEN 'payment_without_order'
         WHEN NOT has_payment AND order_status = 'DELIVERED'                      THEN 'delivered_without_payment'
         WHEN NOT has_payment AND order_status = 'RETURNED'                       THEN 'returned_without_refund'
         WHEN order_status = 'DELIVERED' AND payment_status = 'FAILED'            THEN 'delivered_but_payment_failed'
         WHEN order_status = 'DELIVERED' AND payment_status = 'REFUNDED'          THEN 'delivered_but_payment_refunded'
         WHEN order_status = 'CANCELLED' AND payment_status = 'SUCCESS'           THEN 'cancelled_but_payment_success'
         WHEN order_status = 'RETURNED'  AND payment_status = 'SUCCESS'           THEN 'returned_but_not_refunded'
         WHEN payment_status = 'SUCCESS' AND abs(amount_paid - order_amount) > 0.01 THEN 'amount_mismatch'
         WHEN pay_mode IS NOT NULL AND pay_mode <> order_mode                     THEN 'payment_mode_mismatch'
       END AS reason,
       '$run_id' AS run_id, TIMESTAMP '$now' AS reconciled_at
FROM j
WHERE reason IS NOT NULL
ORDER BY reason, order_id;
