#!/usr/bin/env bash
# Trigger a REAL scheduler run of a DAG with optional conf, wait for it to finish, and dump what is observable:
# task states / tries / durations, the pipeline_runs row, non-passing dq_results, quarantine counts.
#   scripts/experiment.sh <dag_id> ['{"orders_file": "experiments/exp5_orders_bad.csv"}'] [timeout_s]
set -euo pipefail
DAG=$1; CONF=${2:-}; TIMEOUT=${3:-420}
SCHED=$(docker ps -qf name=scheduler | head -1)
PSQL="psql postgresql://postgres:postgres@postgres:5432/postgres -Atc"
if [ -n "$CONF" ]; then OUT=$(astro dev run dags trigger "$DAG" --conf "$CONF" 2>&1); else OUT=$(astro dev run dags trigger "$DAG" 2>&1); fi
RUN=$(echo "$OUT" | grep -oE "run_id=[^ ]+" | head -1 | cut -d= -f2)
echo "▶ $DAG  run_id=$RUN  conf=${CONF:-none}"
for i in $(seq 1 $((TIMEOUT/5))); do
  ST=$(docker exec "$SCHED" $PSQL "select state from dag_run where run_id='$RUN'")
  case "$ST" in success|failed) break;; esac; sleep 5
done
echo "■ dag_run state: $ST"
echo "--- task instances (start → end, seconds, tries, state) ---"
docker exec "$SCHED" $PSQL "select rpad(task_id,28,' ')||' '||coalesce(to_char(start_date,'HH24:MI:SS'),'   -    ')||' → '||coalesce(to_char(end_date,'HH24:MI:SS'),'   -    ')||'  '||lpad(coalesce(round(extract(epoch from end_date-start_date))::text,'-'),4,' ')||'s  try='||try_number||'  '||state from task_instance where run_id='$RUN' order by start_date nulls last, task_id"
echo "--- pipeline_runs ---"
.venv/bin/python scripts/wh.py "select run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status from pipeline_runs where run_id='$RUN'" 2>/dev/null || echo "(no row)"
echo "--- dq_results: non-PASS for this run ---"
.venv/bin/python scripts/wh.py "select status, severity, check_name, rows_failed, rows_checked from dq_results where run_id='$RUN' and status<>'PASS' order by severity, rows_failed desc" 2>/dev/null | head -45
