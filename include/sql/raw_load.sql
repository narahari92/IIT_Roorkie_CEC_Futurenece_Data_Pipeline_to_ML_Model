-- ─────────────────────────────────────────────────────────────────────────────
-- RAW layer (ELT): exact copy of one CSV, every column VARCHAR, plus lineage.
--   _loaded_at   when this run loaded it
--   _source_file which file it came from
--   _run_id      which Airflow run loaded it
--   _row_num     1-based position in the source file (parallel=false keeps order)
-- Append-only: never cleaned, never deleted. Re-running the same run_id is a no-op.
-- Params: $table $file_path $source_file $run_id $now
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS $table AS
SELECT *,
       CAST(NULL AS TIMESTAMP) AS _loaded_at,
       CAST(NULL AS VARCHAR)   AS _source_file,
       CAST(NULL AS VARCHAR)   AS _run_id,
       CAST(NULL AS BIGINT)    AS _row_num
FROM read_csv('$file_path', all_varchar = true, header = true, parallel = false)
WHERE 1 = 0;

INSERT INTO $table BY NAME
SELECT *,
       TIMESTAMP '$now'      AS _loaded_at,
       '$source_file'        AS _source_file,
       '$run_id'             AS _run_id,
       row_number() OVER ()  AS _row_num
FROM read_csv('$file_path', all_varchar = true, header = true, parallel = false)
WHERE NOT EXISTS (SELECT 1 FROM $table WHERE _run_id = '$run_id' AND _source_file = '$source_file');
