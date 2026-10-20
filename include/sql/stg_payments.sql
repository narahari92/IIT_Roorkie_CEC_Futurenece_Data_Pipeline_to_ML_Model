-- ─────────────────────────────────────────────────────────────────────────────
-- STAGING: payments.  Known orders = every order_id ever received in raw_orders
-- (so payments for earlier days are not rejected on an incremental run).
-- Params: $run_id $dag_id $source_file $now $label
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP TABLE _batch AS
WITH src AS (
    SELECT _row_num, _source_file, _run_id,
           NULLIF(trim(payment_id), '')                                AS payment_id,
           NULLIF(trim(order_id), '')                                  AS order_id,
           NULLIF(trim(payment_ts), '')                                AS payment_ts_raw,
           try_strptime(NULLIF(trim(payment_ts), ''), '%Y-%m-%d %H:%M:%S') AS payment_ts,
           CASE lower(regexp_replace(coalesce(payment_mode, ''), '\s', '', 'g'))
                WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'wallet' THEN 'Wallet'
                WHEN 'cashondelivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
                WHEN 'cash' THEN 'Cash on Delivery' WHEN 'cashh' THEN 'Cash on Delivery'
                ELSE NULLIF(trim(payment_mode), '') END                AS payment_mode,
           try_cast(trim(amount_paid) AS DOUBLE)                       AS amount_paid,
           upper(NULLIF(trim(payment_status), ''))                     AS payment_status,
           NULLIF(trim(gateway_ref), '')                               AS gateway_ref
    FROM raw_payments
    WHERE _run_id = '$run_id' AND _source_file = '$source_file'
),
known AS (SELECT (SELECT list(DISTINCT trim(order_id)) FROM raw_orders WHERE order_id IS NOT NULL) AS order_ids),
checks AS (
    SELECT s.*,
        s.payment_id IS NULL                                                                  AS f_payment_id_not_null,
        s.order_id IS NULL                                                                    AS f_order_id_not_null,
        s.amount_paid IS NULL                                                                 AS f_amount_paid_not_null,
        s.gateway_ref IS NULL AND s.payment_mode <> 'Cash on Delivery'                        AS f_gateway_ref_present_for_non_cod,
        row_number() OVER (PARTITION BY s.payment_id ORDER BY s._row_num) > 1                 AS f_payment_id_unique,
        s.payment_ts_raw IS NOT NULL AND s.payment_ts IS NULL                                 AS f_payment_ts_parses,
        s.payment_status IS NOT NULL AND s.payment_status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED') AS f_payment_status_in_list,
        s.payment_mode IS NOT NULL AND s.payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') AS f_payment_mode_in_list,
        s.payment_status = 'SUCCESS' AND s.amount_paid <= 0                                   AS f_success_amount_positive,
        s.order_id IS NOT NULL AND NOT list_contains(k.order_ids, s.order_id)                 AS f_order_id_in_orders,
        o.payment_mode IS NOT NULL AND o.payment_mode <> s.payment_mode                       AS f_mode_matches_order,
        s.payment_status = 'SUCCESS' AND o.order_amount IS NOT NULL
            AND abs(s.amount_paid - o.order_amount) > 0.01                                    AS f_success_amount_equals_order_amount,
        s.payment_ts > TIMESTAMP '$now'                                                       AS f_payment_ts_not_future
    FROM src s
    CROSS JOIN known k
    LEFT JOIN (SELECT order_id, payment_mode, order_amount FROM stg_orders WHERE dq_status = 'PASSED') o USING (order_id)
),
flagged AS (
    SELECT *,
        concat_ws(';',
            CASE WHEN f_payment_id_not_null       THEN 'payments.payment_id_not_null' END,
            CASE WHEN f_order_id_not_null         THEN 'payments.order_id_not_null' END,
            CASE WHEN f_amount_paid_not_null      THEN 'payments.amount_paid_not_null' END,
            CASE WHEN f_payment_id_unique         THEN 'payments.payment_id_unique' END,
            CASE WHEN f_payment_ts_parses         THEN 'payments.payment_ts_parses' END,
            CASE WHEN f_payment_status_in_list    THEN 'payments.payment_status_in_list' END,
            CASE WHEN f_payment_mode_in_list      THEN 'payments.payment_mode_in_list' END,
            CASE WHEN f_success_amount_positive   THEN 'payments.success_amount_positive' END,
            CASE WHEN f_order_id_in_orders        THEN 'payments.order_id_in_orders' END,
            CASE WHEN f_payment_ts_not_future     THEN 'payments.payment_ts_not_future' END)      AS failure_reason,
        NULLIF(concat_ws(';',
            CASE WHEN f_gateway_ref_present_for_non_cod      THEN 'payments.gateway_ref_present_for_non_cod' END,
            CASE WHEN f_mode_matches_order                   THEN 'payments.mode_matches_order' END,
            CASE WHEN f_success_amount_equals_order_amount   THEN 'payments.success_amount_equals_order_amount' END), '') AS dq_warnings
    FROM checks
)
SELECT *, CASE WHEN failure_reason = '' THEN 'PASSED' ELSE 'FAILED' END AS dq_status FROM flagged;

DELETE FROM stg_rejections WHERE run_id = '$run_id' AND source_file = '$source_file' AND entity = 'payments';
INSERT INTO stg_rejections
SELECT '$run_id', '$source_file', 'payments', b.payment_id, b._row_num, b.failure_reason, b.dq_warnings, to_json(r), TIMESTAMP '$now'
FROM _batch b JOIN raw_payments r ON r._run_id = b._run_id AND r._source_file = b._source_file AND r._row_num = b._row_num
WHERE b.dq_status = 'FAILED';

CREATE TABLE IF NOT EXISTS stg_payments (
    payment_id VARCHAR, order_id VARCHAR, payment_ts TIMESTAMP, payment_mode VARCHAR, amount_paid DOUBLE,
    payment_status VARCHAR, gateway_ref VARCHAR,
    dq_status VARCHAR, failure_reason VARCHAR, dq_warnings VARCHAR,
    _source_file VARCHAR, _run_id VARCHAR, _row_num BIGINT, _staged_at TIMESTAMP);

MERGE INTO stg_payments t
USING (
    SELECT payment_id, order_id, payment_ts, payment_mode, amount_paid, payment_status, gateway_ref,
           dq_status, NULLIF(failure_reason, '') AS failure_reason, dq_warnings,
           _source_file, _run_id, _row_num, TIMESTAMP '$now' AS _staged_at
    FROM _batch WHERE payment_id IS NOT NULL AND NOT f_payment_id_unique
) s ON t.payment_id = s.payment_id
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT;

DELETE FROM dq_results WHERE run_id = '$run_id' AND dag_id = '$dag_id' AND check_name LIKE '$label.%';
INSERT INTO dq_results
SELECT '$run_id', '$dag_id', '$label.' || c.chk, c.dimension, c.severity, (SELECT count(*) FROM _batch), c.failed,
       CASE WHEN c.failed = 0 THEN 'PASS' WHEN c.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END, TIMESTAMP '$now'
FROM (
    SELECT 'payment_id_not_null' AS chk, 'Completeness' AS dimension, 'Critical' AS severity, count(*) FILTER (WHERE f_payment_id_not_null) failed FROM _batch
    UNION ALL SELECT 'order_id_not_null', 'Completeness', 'Critical', count(*) FILTER (WHERE f_order_id_not_null) FROM _batch
    UNION ALL SELECT 'amount_paid_not_null', 'Completeness', 'Critical', count(*) FILTER (WHERE f_amount_paid_not_null) FROM _batch
    UNION ALL SELECT 'gateway_ref_present_for_non_cod', 'Completeness', 'Warning', count(*) FILTER (WHERE f_gateway_ref_present_for_non_cod) FROM _batch
    UNION ALL SELECT 'payment_id_unique', 'Uniqueness', 'Critical', count(*) FILTER (WHERE f_payment_id_unique) FROM _batch
    UNION ALL SELECT 'payment_ts_parses', 'Validity', 'Critical', count(*) FILTER (WHERE f_payment_ts_parses) FROM _batch
    UNION ALL SELECT 'payment_status_in_list', 'Validity', 'Critical', count(*) FILTER (WHERE f_payment_status_in_list) FROM _batch
    UNION ALL SELECT 'payment_mode_in_list', 'Validity', 'Critical', count(*) FILTER (WHERE f_payment_mode_in_list) FROM _batch
    UNION ALL SELECT 'success_amount_positive', 'Accuracy', 'Critical', count(*) FILTER (WHERE f_success_amount_positive) FROM _batch
    UNION ALL SELECT 'order_id_in_orders', 'Consistency', 'Critical', count(*) FILTER (WHERE f_order_id_in_orders) FROM _batch
    UNION ALL SELECT 'mode_matches_order', 'Consistency', 'Warning', count(*) FILTER (WHERE f_mode_matches_order) FROM _batch
    UNION ALL SELECT 'success_amount_equals_order_amount', 'Consistency', 'Warning', count(*) FILTER (WHERE f_success_amount_equals_order_amount) FROM _batch
    UNION ALL SELECT 'payment_ts_not_future', 'Timeliness', 'Critical', count(*) FILTER (WHERE f_payment_ts_not_future) FROM _batch
    UNION ALL SELECT 'latest_payment_not_stale', 'Timeliness', 'Warning',
                     CASE WHEN max(payment_ts) FILTER (WHERE payment_ts <= TIMESTAMP '$now') < TIMESTAMP '$now' - INTERVAL 1 DAY
                          OR max(payment_ts) FILTER (WHERE payment_ts <= TIMESTAMP '$now') IS NULL THEN 1 ELSE 0 END FROM _batch
) c;
