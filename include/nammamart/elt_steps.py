"""
Step functions for the NammaMart ELT DAG: Extract → Load raw → Transform in DuckDB with SQL.
Every transformation is a .sql file in include/sql/ rendered with $params and executed as-is.
Python here only orchestrates: it chooses the file, passes parameters and prints counts.

Local run (no Airflow):  AIRFLOW_HOME=$PWD .venv/bin/python -m include.nammamart.elt_steps
"""
from __future__ import annotations

import pendulum

from . import config as C
from . import warehouse as W

DAG_ID = f"nammamart_elt_{C.NAME}"
RAW_TABLES = {e: f"raw_{e}" for e in C.SOURCE_FILES}
MARTS = ["mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
         "mart_payment_reconciliation", "mart_payment_mode_revenue", "mart_top_customers", "mart_rejections"]
EXPECTED_COLUMNS = {
    "orders": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price",
               "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"],
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status", "gateway_ref"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode", "signup_date", "loyalty_tier", "is_active"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit", "is_perishable", "launch_date"],
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name", "capacity_orders_per_day"],
}


def _now_str() -> str:
    return pendulum.now(C.TZ).format("YYYY-MM-DD HH:mm:ss")


# ───────────────────────────── RAW ─────────────────────────────
def raw_load(entity: str, file_name: str, run_id: str) -> dict:
    """Append the CSV to raw_<entity> exactly as received (all VARCHAR) + lineage columns."""
    path = C.DATA_DIR / file_name
    if not path.exists():
        raise FileNotFoundError(f"[Raw:{entity}] source file not found: {path}")
    table = RAW_TABLES[entity]
    with W.connect() as con:
        # schema contract: fail loudly if the export's columns changed
        cols = [r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_csv('{path}', all_varchar=true, header=true)").fetchall()]
        missing = [c for c in EXPECTED_COLUMNS[entity] if c not in cols]
        extra = [c for c in cols if c not in EXPECTED_COLUMNS[entity]]
        if missing or extra:
            raise ValueError(f"[Raw:{entity}] schema check failed for {file_name}: missing={missing} unexpected={extra}")
        before = _count(con, table)
        W.run_sql_file(con, "raw_load", table=table, file_path=str(path), source_file=file_name,
                       run_id=run_id, now=_now_str())
        after = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        this_run = con.execute(f"SELECT COUNT(*) FROM {table} WHERE _run_id = ? AND _source_file = ?",
                               [run_id, file_name]).fetchone()[0]
    print(f"[Raw:{entity}] {file_name}: appended {after - before} rows (this run/file holds {this_run}); "
          f"{table} now {after} rows (before {before})")
    return {"entity": entity, "source_file": file_name, "table": table, "rows_in": this_run,
            "appended": after - before, "table_rows": after}


def _count(con, table) -> int:
    exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table]).fetchone()[0]
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] if exists else 0


# ───────────────────────────── STAGING ─────────────────────────────
def stage(entity: str, source_file: str, run_id: str, label: str | None = None) -> dict:
    """Run include/sql/stg_<entity>.sql: standardise, quality rules, dedupe, mask, MERGE, dq_results."""
    label = label or entity
    with W.connect() as con:
        W.run_sql_file(con, "stg_common_ddl")
        before = _count(con, f"stg_{entity}")
        W.run_sql_file(con, f"stg_{entity}", run_id=run_id, dag_id=DAG_ID, source_file=source_file,
                       now=_now_str(), label=label)
        after = con.execute(f"SELECT COUNT(*) FROM stg_{entity}").fetchone()[0]
        rows_in = con.execute(f"SELECT COUNT(*) FROM raw_{entity} WHERE _run_id = ? AND _source_file = ?",
                              [run_id, source_file]).fetchone()[0]
        rejected = con.execute("SELECT COUNT(*) FROM stg_rejections WHERE run_id = ? AND source_file = ? AND entity = ?",
                               [run_id, source_file, entity]).fetchone()[0]
        reasons = con.execute("""
            SELECT reason, COUNT(*) n FROM (
              SELECT unnest(string_split(failure_reason, ';')) reason FROM stg_rejections
              WHERE run_id = ? AND source_file = ? AND entity = ?) GROUP BY 1 ORDER BY 2 DESC""",
            [run_id, source_file, entity]).fetchall()
        dq = con.execute("""
            SELECT status, severity, dimension, check_name, rows_failed, rows_checked FROM dq_results
            WHERE run_id = ? AND dag_id = ? AND check_name LIKE ? ORDER BY check_name""",
            [run_id, DAG_ID, f"{label}.%"]).fetchall()
    print(f"[Staging:{label}] in={rows_in} passed={rows_in - rejected} rejected={rejected} | "
          f"stg_{entity}: before={before} after={after} (merged {after - before:+d})")
    for status, sev, dim, name, failed, checked in dq:
        print(f"  {status:4} {sev:8} {dim:12} {name:50} failed {failed:>5} / {checked}")
    if reasons:
        print(f"[Staging:{label}] rejection reasons: " + ", ".join(f"{r}={n}" for r, n in reasons))
    return {"entity": entity, "label": label, "source_file": source_file, "rows_in": rows_in,
            "rows_passed": rows_in - rejected, "rows_failed": rejected, "stg_before": before, "stg_after": after}


# ───────────────────────────── MART ─────────────────────────────
def build_mart(name: str, run_id: str) -> dict:
    with W.connect() as con:
        W.run_sql_file(con, name, run_id=run_id, now=_now_str())
        df = con.execute(f"SELECT * FROM {name}").fetchdf()
    print(f"[Mart] {name}: {len(df)} rows")
    print(df.head(12).to_string(index=False))
    return {"table": name, "rows": len(df)}


def compare_with_etl() -> dict:
    """The two pipelines must agree. Compare September delivered orders & net revenue."""
    with W.connect(read_only=True) as con:
        tables = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
        elt = con.execute("""SELECT COUNT(*) FILTER (WHERE order_status='DELIVERED'), ROUND(SUM(CASE WHEN order_status='DELIVERED' THEN order_amount END),2),
                                    COUNT(*) FROM stg_orders WHERE dq_status='PASSED' AND order_date BETWEEN '2026-09-01' AND '2026-09-30'""").fetchone()
        elt_margin = con.execute("SELECT ROUND(SUM(gross_margin),2) FROM mart_daily_store_kpis WHERE order_date BETWEEN '2026-09-01' AND '2026-09-30'").fetchone()[0]
        out = {"elt_sept_delivered": elt[0], "elt_sept_net_revenue": elt[1], "elt_sept_clean_orders": elt[2], "elt_sept_gross_margin": elt_margin}
        if "etl_fact_orders" in tables:
            etl = con.execute("""SELECT COUNT(*) FILTER (WHERE is_delivered), ROUND(SUM(net_revenue),2), COUNT(*), ROUND(SUM(gross_margin),2)
                                 FROM etl_fact_orders WHERE order_date BETWEEN '2026-09-01' AND '2026-09-30'""").fetchone()
            out.update({"etl_sept_delivered": etl[0], "etl_sept_net_revenue": etl[1], "etl_sept_clean_orders": etl[2], "etl_sept_gross_margin": etl[3]})
            out["match"] = (etl[0] == elt[0] and abs((etl[1] or 0) - (elt[1] or 0)) < 0.01 and etl[2] == elt[2]
                            and abs((etl[3] or 0) - (elt_margin or 0)) < 0.01)
        else:
            out["match"] = None
    print("[Compare] September totals, ELT vs ETL:")
    for k, v in out.items():
        print(f"  {k:26} {v}")
    if out["match"] is False:
        print("[Compare] *** MISMATCH *** — investigate before the 9:30 stand-up")
    return out


# ───────────────────────────── local driver ─────────────────────────────
if __name__ == "__main__":
    import sys
    run_id = "local__" + pendulum.now(C.TZ).format("YYYYMMDD_HHmmss")
    inc = sys.argv[1] if len(sys.argv) > 1 else None
    for e, f in C.SOURCE_FILES.items():
        raw_load(e, f, run_id)
    for e in ["customers", "products", "stores", "orders", "payments"]:
        stage(e, C.SOURCE_FILES[e], run_id)
    if inc:
        raw_load("orders", inc, run_id)
        stage("orders", inc, run_id, label="orders_increment")
    for m in MARTS:
        build_mart(m, run_id)
    compare_with_etl()
