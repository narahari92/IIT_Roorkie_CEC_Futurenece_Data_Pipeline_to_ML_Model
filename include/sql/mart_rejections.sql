-- MART: rows rejected per run, by file and by reason (Q6).
CREATE OR REPLACE TABLE mart_rejections AS
SELECT run_id, source_file, entity, reason, count(*) AS rows_rejected
FROM (SELECT run_id, source_file, entity, unnest(string_split(failure_reason, ';')) AS reason FROM stg_rejections)
GROUP BY ALL
ORDER BY run_id, source_file, rows_rejected DESC;
