#!/usr/bin/env python
"""Data-quality scorecard from dq_results (both pipelines).
    .venv/bin/python scripts/dq_scorecard.py            # latest run of each DAG: coverage matrix, parity, failures, trend
"""
import duckdb, pandas as pd
pd.set_option("display.width", 220); pd.set_option("display.max_rows", 200); pd.set_option("display.max_columns", 30)
con = duckdb.connect("include/warehouse_hosur_narahari.duckdb", read_only=True)

latest = con.execute("""
    WITH runs AS (SELECT dag_id, run_id, max(checked_at) AS t FROM dq_results GROUP BY 1,2),
         latest_run AS (SELECT dag_id, run_id FROM (SELECT *, row_number() OVER (PARTITION BY dag_id ORDER BY t DESC) rn FROM runs) WHERE rn = 1)
    SELECT d.* FROM dq_results d JOIN latest_run USING (dag_id, run_id) WHERE check_name NOT LIKE 'orders_increment.%'""").fetchdf()
latest["pipeline"] = latest["dag_id"].str.extract(r"nammamart_(etl|elt)_")[0].str.upper()
latest["file"] = latest["check_name"].str.split(".").str[0]

print("=== 1. Coverage: checks per dimension × source file (latest run, per pipeline) ===")
cov = latest.pivot_table(index=["pipeline", "dimension"], columns="file", values="check_name", aggfunc="count", fill_value=0)
print(cov.to_string())

print("\n=== 2. Severity split per dimension (ETL run) ===")
print(latest[latest.pipeline == "ETL"].pivot_table(index="dimension", columns="severity", values="check_name", aggfunc="count", fill_value=0).to_string())

print("\n=== 3. Parity: same check, same rows_failed in ETL and ELT? ===")
par = latest.pivot_table(index=["check_name", "dimension", "severity"], columns="pipeline", values="rows_failed", aggfunc="first").reset_index()
par["match"] = par["ETL"] == par["ELT"]
print(f"checks in ETL: {latest[latest.pipeline=='ETL'].shape[0]}, in ELT: {latest[latest.pipeline=='ELT'].shape[0]}, "
      f"common: {par.dropna().shape[0]}, identical rows_failed: {par['match'].sum()}, mismatches: {(~par['match']).sum()}")
if (~par["match"]).any():
    print(par[~par["match"]].to_string(index=False))

print("\n=== 4. Non-passing checks (ETL latest run) — the scorecard the client sees ===")
bad = latest[(latest.pipeline == "ETL") & (latest.status != "PASS")].sort_values(["severity", "dimension", "rows_failed"], ascending=[True, True, False])
print(bad[["status", "severity", "dimension", "check_name", "rows_failed", "rows_checked"]].to_string(index=False))

print("\n=== 5. Trend: Critical rows failed per run (both DAGs) ===")
trend = con.execute("""
    SELECT dag_id, run_id, min(checked_at) checked_at,
           sum(rows_failed) FILTER (WHERE severity='Critical') critical_rows_failed,
           sum(rows_failed) FILTER (WHERE severity='Warning')  warning_rows_failed,
           count(*) FILTER (WHERE status='FAIL') checks_failed, count(*) FILTER (WHERE status='WARN') checks_warned, count(*) checks
    FROM dq_results WHERE check_name NOT LIKE 'orders_increment.%' GROUP BY 1,2 ORDER BY 3""").fetchdf()
print(trend.to_string(index=False))
