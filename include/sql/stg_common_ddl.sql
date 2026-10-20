-- Shared ELT control tables (created once, idempotent).
CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR,
    rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP);

-- Every row that failed a Critical rule, as received (raw_row JSON) + why. ELT twin of the ETL quarantine files.
CREATE TABLE IF NOT EXISTS stg_rejections (
    run_id VARCHAR, source_file VARCHAR, entity VARCHAR, row_key VARCHAR, _row_num BIGINT,
    failure_reason VARCHAR, dq_warnings VARCHAR, raw_row JSON, rejected_at TIMESTAMP);
