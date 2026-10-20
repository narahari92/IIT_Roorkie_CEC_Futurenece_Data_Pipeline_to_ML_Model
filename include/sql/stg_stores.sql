-- ─────────────────────────────────────────────────────────────────────────────
-- STAGING: stores.  Blank zone → 'Unknown' (Warning) so the store's revenue is kept.
-- Params: $run_id $dag_id $source_file $now $label
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP TABLE _batch AS
WITH src AS (
    SELECT _row_num, _source_file, _run_id,
           NULLIF(trim(store_id), '')    AS store_id,
           NULLIF(trim(store_name), '')  AS store_name,
           upper(left(NULLIF(trim(zone), ''), 1)) || lower(substr(NULLIF(trim(zone), ''), 2)) AS zone,
           NULLIF(trim(area), '')        AS area,
           NULLIF(trim(pincode), '')     AS pincode,
           NULLIF(trim(open_date), '')   AS open_date,
           NULLIF(trim(manager_name), '') AS manager_name,
           try_cast(trim(capacity_orders_per_day) AS INTEGER) AS capacity_orders_per_day
    FROM raw_stores
    WHERE _run_id = '$run_id' AND _source_file = '$source_file'
),
checks AS (
    SELECT *,
        store_id IS NULL                                                              AS f_store_id_not_null,
        zone IS NULL                                                                  AS f_zone_not_null,
        row_number() OVER (PARTITION BY store_id ORDER BY _row_num) > 1               AS f_store_id_unique,
        pincode IS NOT NULL AND NOT regexp_matches(pincode, '^[1-9][0-9]{5}$')        AS f_pincode_6_digits,
        capacity_orders_per_day IS NULL OR capacity_orders_per_day <= 0               AS f_capacity_positive,
        zone IS NOT NULL AND zone NOT IN ('North', 'South', 'East', 'West')           AS f_zone_in_list,
        try_strptime(open_date, '%Y-%m-%d') > TIMESTAMP '$now'                        AS f_open_date_not_future
    FROM src
),
flagged AS (
    SELECT *,
        concat_ws(';',
            CASE WHEN f_store_id_not_null     THEN 'stores.store_id_not_null' END,
            CASE WHEN f_store_id_unique       THEN 'stores.store_id_unique' END,
            CASE WHEN f_open_date_not_future  THEN 'stores.open_date_not_future' END)   AS failure_reason,
        NULLIF(concat_ws(';',
            CASE WHEN f_zone_not_null         THEN 'stores.zone_not_null' END,
            CASE WHEN f_pincode_6_digits      THEN 'stores.pincode_6_digits' END,
            CASE WHEN f_capacity_positive     THEN 'stores.capacity_positive' END,
            CASE WHEN f_zone_in_list          THEN 'stores.zone_in_list' END), '')      AS dq_warnings
    FROM checks
)
SELECT *, CASE WHEN failure_reason = '' THEN 'PASSED' ELSE 'FAILED' END AS dq_status FROM flagged;

DELETE FROM stg_rejections WHERE run_id = '$run_id' AND source_file = '$source_file' AND entity = 'stores';
INSERT INTO stg_rejections
SELECT '$run_id', '$source_file', 'stores', b.store_id, b._row_num, b.failure_reason, b.dq_warnings, to_json(r), TIMESTAMP '$now'
FROM _batch b JOIN raw_stores r ON r._run_id = b._run_id AND r._source_file = b._source_file AND r._row_num = b._row_num
WHERE b.dq_status = 'FAILED';

CREATE TABLE IF NOT EXISTS stg_stores (
    store_id VARCHAR, store_name VARCHAR, zone VARCHAR, area VARCHAR, pincode VARCHAR, open_date DATE,
    manager_name VARCHAR, capacity_orders_per_day INTEGER,
    dq_status VARCHAR, failure_reason VARCHAR, dq_warnings VARCHAR,
    _source_file VARCHAR, _run_id VARCHAR, _row_num BIGINT, _staged_at TIMESTAMP);

MERGE INTO stg_stores t
USING (
    SELECT store_id, store_name, coalesce(zone, 'Unknown') AS zone, area, pincode,
           CAST(try_strptime(open_date, '%Y-%m-%d') AS DATE) AS open_date, manager_name, capacity_orders_per_day,
           dq_status, NULLIF(failure_reason, '') AS failure_reason, dq_warnings,
           _source_file, _run_id, _row_num, TIMESTAMP '$now' AS _staged_at
    FROM _batch WHERE store_id IS NOT NULL AND NOT f_store_id_unique
) s ON t.store_id = s.store_id
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT;

DELETE FROM dq_results WHERE run_id = '$run_id' AND dag_id = '$dag_id' AND check_name LIKE '$label.%';
INSERT INTO dq_results
SELECT '$run_id', '$dag_id', '$label.' || c.chk, c.dimension, c.severity, (SELECT count(*) FROM _batch), c.failed,
       CASE WHEN c.failed = 0 THEN 'PASS' WHEN c.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END, TIMESTAMP '$now'
FROM (
    SELECT 'store_id_not_null' AS chk, 'Completeness' AS dimension, 'Critical' AS severity, count(*) FILTER (WHERE f_store_id_not_null) failed FROM _batch
    UNION ALL SELECT 'zone_not_null', 'Completeness', 'Warning', count(*) FILTER (WHERE f_zone_not_null) FROM _batch
    UNION ALL SELECT 'store_id_unique', 'Uniqueness', 'Critical', count(*) FILTER (WHERE f_store_id_unique) FROM _batch
    UNION ALL SELECT 'pincode_6_digits', 'Validity', 'Warning', count(*) FILTER (WHERE f_pincode_6_digits) FROM _batch
    UNION ALL SELECT 'capacity_positive', 'Accuracy', 'Warning', count(*) FILTER (WHERE f_capacity_positive) FROM _batch
    UNION ALL SELECT 'zone_in_list', 'Consistency', 'Warning', count(*) FILTER (WHERE f_zone_in_list) FROM _batch
    UNION ALL SELECT 'open_date_not_future', 'Timeliness', 'Critical', count(*) FILTER (WHERE f_open_date_not_future) FROM _batch
) c;
