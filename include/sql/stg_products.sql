-- ─────────────────────────────────────────────────────────────────────────────
-- STAGING: products.  Dedupe rule: a row whose name ends in "(old)" is superseded;
-- otherwise keep the first occurrence. Blank category → 'Unknown' (Warning).
-- Params: $run_id $dag_id $source_file $now $label
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP TABLE _batch AS
WITH src AS (
    SELECT _row_num, _source_file, _run_id,
           NULLIF(trim(product_id), '')     AS product_id,
           NULLIF(trim(product_name), '')   AS product_name,
           NULLIF(trim(category), '')       AS category,
           NULLIF(trim(sub_category), '')   AS sub_category,
           NULLIF(trim(brand), '')          AS brand,
           try_cast(trim(mrp) AS DOUBLE)    AS mrp,
           try_cast(trim(cost_price) AS DOUBLE) AS cost_price,
           NULLIF(trim(unit), '')           AS unit,
           CASE lower(trim(is_perishable)) WHEN 'true' THEN 'True' WHEN '1' THEN 'True'
                                           WHEN 'false' THEN 'False' WHEN '0' THEN 'False'
                                           ELSE NULLIF(trim(is_perishable), '') END AS is_perishable,
           NULLIF(trim(launch_date), '')    AS launch_date
    FROM raw_products
    WHERE _run_id = '$run_id' AND _source_file = '$source_file'
),
checks AS (
    SELECT *,
        product_id IS NULL                                                                             AS f_product_id_not_null,
        category IS NULL                                                                               AS f_category_not_null,
        product_name IS NULL                                                                           AS f_product_name_not_null,
        count(*) OVER (PARTITION BY product_id) > 1 AND product_name LIKE '%(old)'                     AS f_superseded_old_row,
        row_number() OVER (PARTITION BY product_id ORDER BY (product_name LIKE '%(old)'), _row_num) > 1 AS f_product_id_unique,
        is_perishable IS NOT NULL AND is_perishable NOT IN ('True', 'False')                           AS f_is_perishable_bool,
        launch_date IS NOT NULL AND try_strptime(launch_date, '%Y-%m-%d') IS NULL                      AS f_launch_date_parses,
        mrp IS NULL OR mrp <= 0                                                                        AS f_mrp_positive,
        cost_price IS NULL OR cost_price <= 0                                                          AS f_cost_price_positive,
        cost_price > mrp                                                                               AS f_cost_price_not_above_mrp,
        category IS NOT NULL AND category NOT IN ('Fruits & Vegetables', 'Dairy & Bakery', 'Snacks & Beverages',
                                                  'Staples', 'Household', 'Personal Care')            AS f_category_in_known_list,
        try_strptime(launch_date, '%Y-%m-%d') > TIMESTAMP '$now'                                      AS f_launch_date_not_future
    FROM src
),
flagged AS (
    SELECT *,
        concat_ws(';',
            CASE WHEN f_product_id_not_null    THEN 'products.product_id_not_null' END,
            CASE WHEN f_product_name_not_null  THEN 'products.product_name_not_null' END,
            CASE WHEN f_superseded_old_row     THEN 'products.superseded_old_row' END,
            CASE WHEN f_product_id_unique      THEN 'products.product_id_unique' END,
            CASE WHEN f_cost_price_positive    THEN 'products.cost_price_positive' END,
            CASE WHEN f_launch_date_not_future THEN 'products.launch_date_not_future' END)             AS failure_reason,
        NULLIF(concat_ws(';',
            CASE WHEN f_category_not_null        THEN 'products.category_not_null' END,
            CASE WHEN f_is_perishable_bool       THEN 'products.is_perishable_bool' END,
            CASE WHEN f_launch_date_parses       THEN 'products.launch_date_parses' END,
            CASE WHEN f_mrp_positive             THEN 'products.mrp_positive' END,
            CASE WHEN f_cost_price_not_above_mrp THEN 'products.cost_price_not_above_mrp' END,
            CASE WHEN f_category_in_known_list   THEN 'products.category_in_known_list' END), '')      AS dq_warnings
    FROM checks
)
SELECT *, CASE WHEN failure_reason = '' THEN 'PASSED' ELSE 'FAILED' END AS dq_status FROM flagged;

DELETE FROM stg_rejections WHERE run_id = '$run_id' AND source_file = '$source_file' AND entity = 'products';
INSERT INTO stg_rejections
SELECT '$run_id', '$source_file', 'products', b.product_id, b._row_num, b.failure_reason, b.dq_warnings, to_json(r), TIMESTAMP '$now'
FROM _batch b JOIN raw_products r ON r._run_id = b._run_id AND r._source_file = b._source_file AND r._row_num = b._row_num
WHERE b.dq_status = 'FAILED';

CREATE TABLE IF NOT EXISTS stg_products (
    product_id VARCHAR, product_name VARCHAR, category VARCHAR, sub_category VARCHAR, brand VARCHAR,
    mrp DOUBLE, cost_price DOUBLE, unit VARCHAR, is_perishable BOOLEAN, launch_date DATE,
    dq_status VARCHAR, failure_reason VARCHAR, dq_warnings VARCHAR,
    _source_file VARCHAR, _run_id VARCHAR, _row_num BIGINT, _staged_at TIMESTAMP);

MERGE INTO stg_products t
USING (
    SELECT product_id, product_name, coalesce(category, 'Unknown') AS category, sub_category, brand, mrp, cost_price, unit,
           is_perishable = 'True' AS is_perishable, CAST(try_strptime(launch_date, '%Y-%m-%d') AS DATE) AS launch_date,
           dq_status, NULLIF(failure_reason, '') AS failure_reason, dq_warnings,
           _source_file, _run_id, _row_num, TIMESTAMP '$now' AS _staged_at
    FROM _batch WHERE product_id IS NOT NULL AND NOT f_product_id_unique
) s ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT;

DELETE FROM dq_results WHERE run_id = '$run_id' AND dag_id = '$dag_id' AND check_name LIKE '$label.%';
INSERT INTO dq_results
SELECT '$run_id', '$dag_id', '$label.' || c.chk, c.dimension, c.severity, (SELECT count(*) FROM _batch), c.failed,
       CASE WHEN c.failed = 0 THEN 'PASS' WHEN c.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END, TIMESTAMP '$now'
FROM (
    SELECT 'product_id_not_null' AS chk, 'Completeness' AS dimension, 'Critical' AS severity, count(*) FILTER (WHERE f_product_id_not_null) failed FROM _batch
    UNION ALL SELECT 'category_not_null', 'Completeness', 'Warning', count(*) FILTER (WHERE f_category_not_null) FROM _batch
    UNION ALL SELECT 'product_name_not_null', 'Completeness', 'Critical', count(*) FILTER (WHERE f_product_name_not_null) FROM _batch
    UNION ALL SELECT 'superseded_old_row', 'Uniqueness', 'Critical', count(*) FILTER (WHERE f_superseded_old_row) FROM _batch
    UNION ALL SELECT 'product_id_unique', 'Uniqueness', 'Critical', count(*) FILTER (WHERE f_product_id_unique) FROM _batch
    UNION ALL SELECT 'is_perishable_bool', 'Validity', 'Warning', count(*) FILTER (WHERE f_is_perishable_bool) FROM _batch
    UNION ALL SELECT 'launch_date_parses', 'Validity', 'Warning', count(*) FILTER (WHERE f_launch_date_parses) FROM _batch
    UNION ALL SELECT 'mrp_positive', 'Accuracy', 'Warning', count(*) FILTER (WHERE f_mrp_positive) FROM _batch
    UNION ALL SELECT 'cost_price_positive', 'Accuracy', 'Critical', count(*) FILTER (WHERE f_cost_price_positive) FROM _batch
    UNION ALL SELECT 'cost_price_not_above_mrp', 'Accuracy', 'Warning', count(*) FILTER (WHERE f_cost_price_not_above_mrp) FROM _batch
    UNION ALL SELECT 'category_in_known_list', 'Consistency', 'Warning', count(*) FILTER (WHERE f_category_in_known_list) FROM _batch
    UNION ALL SELECT 'launch_date_not_future', 'Timeliness', 'Critical', count(*) FILTER (WHERE f_launch_date_not_future) FROM _batch
) c;
