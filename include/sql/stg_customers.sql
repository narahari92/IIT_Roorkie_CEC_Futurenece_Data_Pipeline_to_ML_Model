-- ─────────────────────────────────────────────────────────────────────────────
-- STAGING: customers.  raw_customers (this run, this file) → standardise → quality
-- rules → dedupe → mask PII → MERGE into stg_customers on customer_id.
-- Params: $run_id $dag_id $source_file $now $label
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP TABLE _batch AS
WITH src AS (                                   -- standardise: trim, blank/N/A → NULL, casing
    SELECT _row_num, _source_file, _run_id,
           NULLIF(NULLIF(trim(customer_id), ''), 'N/A')                         AS customer_id,
           NULLIF(NULLIF(trim(full_name), ''), 'N/A')                           AS full_name,
           lower(NULLIF(NULLIF(trim(email), ''), 'N/A'))                        AS email,
           NULLIF(NULLIF(trim(phone), ''), 'N/A')                               AS phone,
           upper(left(NULLIF(trim(city), ''), 1)) || lower(substr(NULLIF(trim(city), ''), 2)) AS city,
           NULLIF(trim(area), '')                                               AS area,
           NULLIF(trim(pincode), '')                                            AS pincode,
           NULLIF(trim(signup_date), '')                                        AS signup_date,
           trim(loyalty_tier)                                                   AS loyalty_tier_raw,
           upper(left(NULLIF(trim(loyalty_tier), ''), 1)) || lower(substr(NULLIF(trim(loyalty_tier), ''), 2)) AS loyalty_tier,
           upper(NULLIF(trim(is_active), ''))                                   AS is_active
    FROM raw_customers
    WHERE _run_id = '$run_id' AND _source_file = '$source_file'
),
first_orders AS (
    SELECT trim(customer_id) AS customer_id, min(try_strptime(order_ts, '%Y-%m-%d %H:%M:%S')) AS first_order_ts
    FROM raw_orders GROUP BY 1
),
checks AS (
    SELECT s.*,
        -- Completeness
        s.customer_id IS NULL                                                                        AS f_customer_id_not_null,
        s.full_name IS NULL                                                                          AS f_full_name_not_null,
        s.city IS NULL                                                                               AS f_city_not_null,
        s.email IS NULL                                                                              AS f_email_not_null,
        -- Uniqueness (export is chronological → the LAST row is the current record)
        row_number() OVER (PARTITION BY s.customer_id ORDER BY s._row_num DESC) > 1                  AS f_customer_id_unique,
        -- Validity
        s.email IS NOT NULL AND NOT regexp_matches(s.email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') AS f_email_format,
        s.phone IS NOT NULL AND NOT regexp_matches(s.phone, '^[6-9][0-9]{9}$')                       AS f_phone_10_digits,
        s.pincode IS NOT NULL AND NOT regexp_matches(s.pincode, '^[1-9][0-9]{5}$')                   AS f_pincode_6_digits,
        s.is_active IS NOT NULL AND s.is_active NOT IN ('Y', 'N')                                    AS f_is_active_Y_N,
        s.signup_date IS NOT NULL AND try_strptime(s.signup_date, '%Y-%m-%d') IS NULL                AS f_signup_date_parses,
        -- Consistency
        s.loyalty_tier IS NOT NULL AND s.loyalty_tier NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum') AS f_loyalty_tier_in_list,
        s.loyalty_tier_raw IS NOT NULL AND s.loyalty_tier_raw <> s.loyalty_tier                      AS f_loyalty_tier_casing_was_fixed,
        -- Timeliness / Accuracy
        try_strptime(s.signup_date, '%Y-%m-%d') > TIMESTAMP '$now'                                   AS f_signup_date_not_future,
        try_strptime(s.signup_date, '%Y-%m-%d') > fo.first_order_ts                                  AS f_signup_not_after_first_order
    FROM src s LEFT JOIN first_orders fo USING (customer_id)
),
flagged AS (
    SELECT *,
        concat_ws(';',
            CASE WHEN f_customer_id_not_null THEN 'customers.customer_id_not_null' END,
            CASE WHEN f_customer_id_unique   THEN 'customers.customer_id_unique' END,
            CASE WHEN f_signup_date_parses   THEN 'customers.signup_date_parses' END,
            CASE WHEN f_loyalty_tier_in_list THEN 'customers.loyalty_tier_in_list' END)              AS failure_reason,
        NULLIF(concat_ws(';',
            CASE WHEN f_full_name_not_null            THEN 'customers.full_name_not_null' END,
            CASE WHEN f_city_not_null                 THEN 'customers.city_not_null' END,
            CASE WHEN f_email_not_null                THEN 'customers.email_not_null' END,
            CASE WHEN f_email_format                  THEN 'customers.email_format' END,
            CASE WHEN f_phone_10_digits               THEN 'customers.phone_10_digits' END,
            CASE WHEN f_pincode_6_digits              THEN 'customers.pincode_6_digits' END,
            CASE WHEN f_is_active_Y_N                 THEN 'customers.is_active_Y_N' END,
            CASE WHEN f_loyalty_tier_casing_was_fixed THEN 'customers.loyalty_tier_casing_was_fixed' END,
            CASE WHEN f_signup_date_not_future        THEN 'customers.signup_date_not_future' END,
            CASE WHEN f_signup_not_after_first_order  THEN 'customers.signup_not_after_first_order' END), '') AS dq_warnings
    FROM checks
)
SELECT *, CASE WHEN failure_reason = '' THEN 'PASSED' ELSE 'FAILED' END AS dq_status FROM flagged;

-- Rejections (as received): idempotent per run/file
DELETE FROM stg_rejections WHERE run_id = '$run_id' AND source_file = '$source_file' AND entity = 'customers';
INSERT INTO stg_rejections
SELECT '$run_id', '$source_file', 'customers', b.customer_id, b._row_num, b.failure_reason, b.dq_warnings, to_json(r), TIMESTAMP '$now'
FROM _batch b JOIN raw_customers r ON r._run_id = b._run_id AND r._source_file = b._source_file AND r._row_num = b._row_num
WHERE b.dq_status = 'FAILED';

-- Typed, masked target
CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id VARCHAR, full_name VARCHAR, email_masked VARCHAR, phone_masked VARCHAR, city VARCHAR, area VARCHAR,
    pincode VARCHAR, signup_date DATE, loyalty_tier VARCHAR, is_active BOOLEAN,
    dq_status VARCHAR, failure_reason VARCHAR, dq_warnings VARCHAR,
    _source_file VARCHAR, _run_id VARCHAR, _row_num BIGINT, _staged_at TIMESTAMP);

MERGE INTO stg_customers t
USING (
    SELECT customer_id, full_name,
           CASE WHEN email IS NULL THEN NULL
                WHEN position('@' IN email) = 0 THEN '***'
                ELSE left(split_part(email, '@', 1), 2) || '***@' || split_part(email, '@', 2) END       AS email_masked,
           CASE WHEN phone IS NULL THEN NULL
                WHEN length(phone) <= 4 THEN repeat('*', length(phone))
                ELSE left(phone, 2) || repeat('*', length(phone) - 4) || right(phone, 2) END               AS phone_masked,
           city, area, pincode,
           CAST(try_strptime(signup_date, '%Y-%m-%d') AS DATE) AS signup_date,
           loyalty_tier, is_active = 'Y' AS is_active,
           dq_status, NULLIF(failure_reason, '') AS failure_reason, dq_warnings,
           _source_file, _run_id, _row_num, TIMESTAMP '$now' AS _staged_at
    FROM _batch
    WHERE customer_id IS NOT NULL AND NOT f_customer_id_unique          -- one survivor per key
) s ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT;

-- Quality scorecard
DELETE FROM dq_results WHERE run_id = '$run_id' AND dag_id = '$dag_id' AND check_name LIKE '$label.%';
INSERT INTO dq_results
SELECT '$run_id', '$dag_id', '$label.' || c.chk, c.dimension, c.severity, (SELECT count(*) FROM _batch), c.failed,
       CASE WHEN c.failed = 0 THEN 'PASS' WHEN c.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END, TIMESTAMP '$now'
FROM (
    SELECT 'customer_id_not_null' AS chk, 'Completeness' AS dimension, 'Critical' AS severity, count(*) FILTER (WHERE f_customer_id_not_null) failed FROM _batch
    UNION ALL SELECT 'full_name_not_null', 'Completeness', 'Warning', count(*) FILTER (WHERE f_full_name_not_null) FROM _batch
    UNION ALL SELECT 'city_not_null', 'Completeness', 'Warning', count(*) FILTER (WHERE f_city_not_null) FROM _batch
    UNION ALL SELECT 'email_not_null', 'Completeness', 'Warning', count(*) FILTER (WHERE f_email_not_null) FROM _batch
    UNION ALL SELECT 'customer_id_unique', 'Uniqueness', 'Critical', count(*) FILTER (WHERE f_customer_id_unique) FROM _batch
    UNION ALL SELECT 'email_format', 'Validity', 'Warning', count(*) FILTER (WHERE f_email_format) FROM _batch
    UNION ALL SELECT 'phone_10_digits', 'Validity', 'Warning', count(*) FILTER (WHERE f_phone_10_digits) FROM _batch
    UNION ALL SELECT 'pincode_6_digits', 'Validity', 'Warning', count(*) FILTER (WHERE f_pincode_6_digits) FROM _batch
    UNION ALL SELECT 'is_active_Y_N', 'Validity', 'Warning', count(*) FILTER (WHERE f_is_active_Y_N) FROM _batch
    UNION ALL SELECT 'signup_date_parses', 'Validity', 'Critical', count(*) FILTER (WHERE f_signup_date_parses) FROM _batch
    UNION ALL SELECT 'loyalty_tier_in_list', 'Consistency', 'Critical', count(*) FILTER (WHERE f_loyalty_tier_in_list) FROM _batch
    UNION ALL SELECT 'loyalty_tier_casing_was_fixed', 'Consistency', 'Warning', count(*) FILTER (WHERE f_loyalty_tier_casing_was_fixed) FROM _batch
    UNION ALL SELECT 'signup_date_not_future', 'Timeliness', 'Warning', count(*) FILTER (WHERE f_signup_date_not_future) FROM _batch
    UNION ALL SELECT 'signup_not_after_first_order', 'Accuracy', 'Warning', count(*) FILTER (WHERE f_signup_not_after_first_order) FROM _batch
) c;
