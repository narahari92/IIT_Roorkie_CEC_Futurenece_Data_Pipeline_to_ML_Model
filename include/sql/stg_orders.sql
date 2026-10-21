-- ─────────────────────────────────────────────────────────────────────────────
-- STAGING: orders.  Same rules as include/nammamart/quality.py ORDER_CHECKS.
-- Runs once for the daily file and once more for the increment file; the MERGE
-- on order_id makes a corrected order (DELIVERED → RETURNED) overwrite its old row
-- and a new order insert — never a duplicate.
-- Params: $run_id $dag_id $source_file $now $label
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP TABLE _batch AS
WITH src AS (
    SELECT _row_num, _source_file, _run_id,
           NULLIF(trim(order_id), '')                                    AS order_id,
           NULLIF(NULLIF(trim(order_ts), ''), 'N/A')                     AS order_ts_raw,
           try_strptime(NULLIF(NULLIF(trim(order_ts), ''), 'N/A'), '%Y-%m-%d %H:%M:%S') AS order_ts,
           NULLIF(trim(customer_id), '')                                 AS customer_id,
           NULLIF(trim(store_id), '')                                    AS store_id,
           NULLIF(trim(product_id), '')                                  AS product_id,
           try_cast(trim(quantity) AS INTEGER)                           AS quantity,
           try_cast(trim(unit_price) AS DOUBLE)                          AS unit_price,
           try_cast(trim(discount_pct) AS DOUBLE)                        AS discount_pct,
           try_cast(trim(order_amount) AS DOUBLE)                        AS order_amount,
           CASE lower(regexp_replace(coalesce(payment_mode, ''), '\s', '', 'g'))
                WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'wallet' THEN 'Wallet'
                WHEN 'cashondelivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
                WHEN 'cash' THEN 'Cash on Delivery' WHEN 'cashh' THEN 'Cash on Delivery'
                ELSE NULLIF(trim(payment_mode), '') END                  AS payment_mode,
           upper(NULLIF(trim(order_status), ''))                         AS order_status,
           upper(left(NULLIF(trim(channel), ''), 1)) || lower(substr(NULLIF(trim(channel), ''), 2)) AS channel,
           try_cast(trim(delivery_minutes) AS INTEGER)                   AS delivery_minutes
    FROM raw_orders
    WHERE _run_id = '$run_id' AND _source_file = '$source_file'
),
masters AS (
    SELECT (SELECT list(DISTINCT trim(customer_id)) FROM raw_customers WHERE customer_id IS NOT NULL)      AS cust_raw,
           (SELECT list(customer_id) FROM stg_customers WHERE dq_status = 'PASSED')                        AS cust_ok,
           (SELECT list(DISTINCT trim(product_id)) FROM raw_products WHERE product_id IS NOT NULL)         AS prod_raw,
           (SELECT list(product_id) FROM stg_products WHERE dq_status = 'PASSED')                          AS prod_ok,
           (SELECT list(DISTINCT trim(store_id)) FROM raw_stores WHERE store_id IS NOT NULL)               AS store_raw,
           (SELECT list(store_id) FROM stg_stores WHERE dq_status = 'PASSED')                              AS store_ok
),
checks AS (
    SELECT s.*,
        -- Completeness
        s.order_id IS NULL                                                                   AS f_order_id_not_null,
        s.customer_id IS NULL                                                                AS f_customer_id_not_null,
        s.order_ts_raw IS NULL                                                               AS f_order_ts_not_null,
        s.order_status = 'DELIVERED' AND s.delivery_minutes IS NULL                          AS f_delivered_has_delivery_minutes,
        -- Uniqueness (keep first occurrence)
        row_number() OVER (PARTITION BY s.order_id ORDER BY s._row_num) > 1                  AS f_order_id_unique,
        -- Validity
        s.order_ts_raw IS NOT NULL AND s.order_ts IS NULL                                    AS f_order_ts_parses,
        s.order_status IS NOT NULL AND s.order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') AS f_order_status_in_list,
        s.payment_mode IS NOT NULL AND s.payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') AS f_payment_mode_in_list,
        s.discount_pct IS NOT NULL AND s.discount_pct NOT BETWEEN 0 AND 100                  AS f_discount_pct_0_100,
        s.channel IS NOT NULL AND s.channel NOT IN ('App', 'Web')                            AS f_channel_in_list,
        -- Accuracy
        s.quantity IS NULL OR s.quantity <= 0                                                AS f_quantity_positive,
        s.unit_price IS NULL OR s.unit_price <= 0                                            AS f_unit_price_positive,
        s.quantity IS NOT NULL AND s.unit_price IS NOT NULL AND s.discount_pct IS NOT NULL AND s.order_amount IS NOT NULL
            AND abs(round(s.quantity * s.unit_price * (1 - s.discount_pct / 100), 2) - s.order_amount) > 0.01 AS f_order_amount_matches_formula,
        s.delivery_minutes >= 120                                                            AS f_delivery_minutes_below_120,
        s.delivery_minutes IS NOT NULL AND s.delivery_minutes < 120 AND s.delivery_minutes NOT BETWEEN 8 AND 35 AS f_delivery_minutes_in_normal_range,
        -- Consistency
        s.customer_id IS NOT NULL AND NOT list_contains(m.cust_raw, s.customer_id)           AS f_customer_id_in_master,
        s.product_id IS NOT NULL AND NOT list_contains(m.prod_raw, s.product_id)             AS f_product_id_in_master,
        s.store_id IS NOT NULL AND NOT list_contains(m.store_raw, s.store_id)                AS f_store_id_in_master,
        s.customer_id IS NOT NULL AND list_contains(m.cust_raw, s.customer_id) AND NOT list_contains(m.cust_ok, s.customer_id) AS f_customer_row_quarantined,
        s.product_id IS NOT NULL AND list_contains(m.prod_raw, s.product_id) AND NOT list_contains(m.prod_ok, s.product_id)     AS f_product_row_quarantined,
        s.store_id IS NOT NULL AND list_contains(m.store_raw, s.store_id) AND NOT list_contains(m.store_ok, s.store_id)         AS f_store_row_quarantined,
        s.unit_price > (SELECT p.mrp FROM stg_products p WHERE p.product_id = s.product_id)  AS f_unit_price_not_above_mrp,
        -- Timeliness
        s.order_ts > TIMESTAMP '$now'                                                        AS f_order_ts_not_future
    FROM src s CROSS JOIN masters m
),
flagged AS (
    SELECT *,
        concat_ws(';',
            CASE WHEN f_order_id_not_null             THEN 'orders.order_id_not_null' END,
            CASE WHEN f_customer_id_not_null          THEN 'orders.customer_id_not_null' END,
            CASE WHEN f_order_ts_not_null             THEN 'orders.order_ts_not_null' END,
            CASE WHEN f_delivered_has_delivery_minutes THEN 'orders.delivered_has_delivery_minutes' END,
            CASE WHEN f_order_id_unique               THEN 'orders.order_id_unique' END,
            CASE WHEN f_order_ts_parses               THEN 'orders.order_ts_parses' END,
            CASE WHEN f_order_status_in_list          THEN 'orders.order_status_in_list' END,
            CASE WHEN f_payment_mode_in_list          THEN 'orders.payment_mode_in_list' END,
            CASE WHEN f_discount_pct_0_100            THEN 'orders.discount_pct_0_100' END,
            CASE WHEN f_quantity_positive             THEN 'orders.quantity_positive' END,
            CASE WHEN f_unit_price_positive           THEN 'orders.unit_price_positive' END,
            CASE WHEN f_order_amount_matches_formula  THEN 'orders.order_amount_matches_formula' END,
            CASE WHEN f_delivery_minutes_below_120    THEN 'orders.delivery_minutes_below_120' END,
            CASE WHEN f_customer_id_in_master         THEN 'orders.customer_id_in_master' END,
            CASE WHEN f_product_id_in_master          THEN 'orders.product_id_in_master' END,
            CASE WHEN f_store_id_in_master            THEN 'orders.store_id_in_master' END,
            CASE WHEN f_customer_row_quarantined      THEN 'orders.customer_row_quarantined' END,
            CASE WHEN f_product_row_quarantined       THEN 'orders.product_row_quarantined' END,
            CASE WHEN f_store_row_quarantined         THEN 'orders.store_row_quarantined' END,
            CASE WHEN f_order_ts_not_future           THEN 'orders.order_ts_not_future' END)    AS failure_reason,
        NULLIF(concat_ws(';',
            CASE WHEN f_channel_in_list                   THEN 'orders.channel_in_list' END,
            CASE WHEN f_delivery_minutes_in_normal_range  THEN 'orders.delivery_minutes_in_normal_range' END,
            CASE WHEN f_unit_price_not_above_mrp          THEN 'orders.unit_price_not_above_mrp' END), '') AS dq_warnings
    FROM checks
)
SELECT *, CASE WHEN failure_reason = '' THEN 'PASSED' ELSE 'FAILED' END AS dq_status FROM flagged;

DELETE FROM stg_rejections WHERE run_id = '$run_id' AND source_file = '$source_file' AND entity = 'orders';
INSERT INTO stg_rejections
SELECT '$run_id', '$source_file', 'orders', b.order_id, b._row_num, b.failure_reason, b.dq_warnings, to_json(r), TIMESTAMP '$now'
FROM _batch b JOIN raw_orders r ON r._run_id = b._run_id AND r._source_file = b._source_file AND r._row_num = b._row_num
WHERE b.dq_status = 'FAILED';

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id VARCHAR, order_ts TIMESTAMP, order_date DATE, customer_id VARCHAR, store_id VARCHAR, product_id VARCHAR,
    quantity INTEGER, unit_price DOUBLE, discount_pct DOUBLE, order_amount DOUBLE, payment_mode VARCHAR,
    order_status VARCHAR, channel VARCHAR, delivery_minutes INTEGER,
    dq_status VARCHAR, failure_reason VARCHAR, dq_warnings VARCHAR,
    _source_file VARCHAR, _run_id VARCHAR, _row_num BIGINT, _staged_at TIMESTAMP, _effective_date DATE);

-- ★ The MERGE: corrections overwrite, new orders insert, duplicates never land.
--   Guard against a stale snapshot undoing a later correction:
--     (a) business rule — a terminal status (RETURNED/CANCELLED) is never regressed to DELIVERED;
--     (b) if both files carry a date in their name, an older file never overwrites a newer one.
--   _effective_date = date in the file name (orders_2026-10-01_increment.csv → 2026-10-01), else NULL.
MERGE INTO stg_orders t
USING (
    SELECT order_id, order_ts, CAST(order_ts AS DATE) AS order_date, customer_id, store_id, product_id,
           quantity, unit_price, discount_pct, order_amount, payment_mode, order_status, channel, delivery_minutes,
           dq_status, NULLIF(failure_reason, '') AS failure_reason, dq_warnings,
           _source_file, _run_id, _row_num, TIMESTAMP '$now' AS _staged_at,
           try_cast(NULLIF(regexp_extract('$source_file', '(\d{4}-\d{2}-\d{2})', 1), '') AS DATE) AS _effective_date
    FROM _batch WHERE order_id IS NOT NULL AND NOT f_order_id_unique
) s ON t.order_id = s.order_id
WHEN MATCHED AND NOT (t.order_status IN ('RETURNED', 'CANCELLED') AND s.order_status NOT IN ('RETURNED', 'CANCELLED'))
             AND coalesce(s._effective_date >= t._effective_date, TRUE) THEN UPDATE
WHEN NOT MATCHED THEN INSERT;

DELETE FROM dq_results WHERE run_id = '$run_id' AND dag_id = '$dag_id' AND check_name LIKE '$label.%';
INSERT INTO dq_results
SELECT '$run_id', '$dag_id', '$label.' || c.chk, c.dimension, c.severity, (SELECT count(*) FROM _batch), c.failed,
       CASE WHEN c.failed = 0 THEN 'PASS' WHEN c.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END, TIMESTAMP '$now'
FROM (
    SELECT 'order_id_not_null' AS chk, 'Completeness' AS dimension, 'Critical' AS severity, count(*) FILTER (WHERE f_order_id_not_null) failed FROM _batch
    UNION ALL SELECT 'customer_id_not_null', 'Completeness', 'Critical', count(*) FILTER (WHERE f_customer_id_not_null) FROM _batch
    UNION ALL SELECT 'order_ts_not_null', 'Completeness', 'Critical', count(*) FILTER (WHERE f_order_ts_not_null) FROM _batch
    UNION ALL SELECT 'delivered_has_delivery_minutes', 'Completeness', 'Critical', count(*) FILTER (WHERE f_delivered_has_delivery_minutes) FROM _batch
    UNION ALL SELECT 'order_id_unique', 'Uniqueness', 'Critical', count(*) FILTER (WHERE f_order_id_unique) FROM _batch
    UNION ALL SELECT 'order_ts_parses', 'Validity', 'Critical', count(*) FILTER (WHERE f_order_ts_parses) FROM _batch
    UNION ALL SELECT 'order_status_in_list', 'Validity', 'Critical', count(*) FILTER (WHERE f_order_status_in_list) FROM _batch
    UNION ALL SELECT 'payment_mode_in_list', 'Validity', 'Critical', count(*) FILTER (WHERE f_payment_mode_in_list) FROM _batch
    UNION ALL SELECT 'discount_pct_0_100', 'Validity', 'Critical', count(*) FILTER (WHERE f_discount_pct_0_100) FROM _batch
    UNION ALL SELECT 'channel_in_list', 'Validity', 'Warning', count(*) FILTER (WHERE f_channel_in_list) FROM _batch
    UNION ALL SELECT 'quantity_positive', 'Accuracy', 'Critical', count(*) FILTER (WHERE f_quantity_positive) FROM _batch
    UNION ALL SELECT 'unit_price_positive', 'Accuracy', 'Critical', count(*) FILTER (WHERE f_unit_price_positive) FROM _batch
    UNION ALL SELECT 'order_amount_matches_formula', 'Accuracy', 'Critical', count(*) FILTER (WHERE f_order_amount_matches_formula) FROM _batch
    UNION ALL SELECT 'delivery_minutes_below_120', 'Accuracy', 'Critical', count(*) FILTER (WHERE f_delivery_minutes_below_120) FROM _batch
    UNION ALL SELECT 'delivery_minutes_in_normal_range', 'Accuracy', 'Warning', count(*) FILTER (WHERE f_delivery_minutes_in_normal_range) FROM _batch
    UNION ALL SELECT 'customer_id_in_master', 'Consistency', 'Critical', count(*) FILTER (WHERE f_customer_id_in_master) FROM _batch
    UNION ALL SELECT 'product_id_in_master', 'Consistency', 'Critical', count(*) FILTER (WHERE f_product_id_in_master) FROM _batch
    UNION ALL SELECT 'store_id_in_master', 'Consistency', 'Critical', count(*) FILTER (WHERE f_store_id_in_master) FROM _batch
    UNION ALL SELECT 'customer_row_quarantined', 'Consistency', 'Critical', count(*) FILTER (WHERE f_customer_row_quarantined) FROM _batch
    UNION ALL SELECT 'product_row_quarantined', 'Consistency', 'Critical', count(*) FILTER (WHERE f_product_row_quarantined) FROM _batch
    UNION ALL SELECT 'store_row_quarantined', 'Consistency', 'Critical', count(*) FILTER (WHERE f_store_row_quarantined) FROM _batch
    UNION ALL SELECT 'unit_price_not_above_mrp', 'Consistency', 'Warning', count(*) FILTER (WHERE f_unit_price_not_above_mrp) FROM _batch
    UNION ALL SELECT 'order_ts_not_future', 'Timeliness', 'Critical', count(*) FILTER (WHERE f_order_ts_not_future) FROM _batch
    -- dataset-level checks (0/1)
    UNION ALL SELECT 'latest_order_not_stale', 'Timeliness', 'Warning',
                     CASE WHEN max(order_ts) FILTER (WHERE order_ts <= TIMESTAMP '$now') < TIMESTAMP '$now' - INTERVAL 1 DAY
                          OR max(order_ts) FILTER (WHERE order_ts <= TIMESTAMP '$now') IS NULL THEN 1 ELSE 0 END FROM _batch
    UNION ALL SELECT 'volume_not_anomalous', 'Timeliness', 'Warning', CASE WHEN count(*) < 500 THEN 1 ELSE 0 END FROM _batch
) c;
