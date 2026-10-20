"""
dags/nammamart_elt_hosur_narahari.py
------------------------------------
NammaMart ELT pipeline — Extract → Load RAW (as received) → Transform in DuckDB with SQL.

  raw_customers ─► stg_customers ─┐
  raw_products  ─► stg_products  ─┼─► stg_orders ─► stg_payments ─► raw_increment ─► stg_merge_increment
  raw_stores    ─► stg_stores    ─┤                                   (skipped unless `increment_file` is set)
  raw_orders    ──────────────────┘                                                     │
  raw_payments  ───────────────────────────────────────────────────────────────────────┘
                                                                                        ▼
        mart_daily_store_kpis ─► mart_category_margin ─► mart_delivery_sla ─► mart_payment_reconciliation
                              ─► mart_payment_mode_revenue ─► mart_top_customers ─► mart_rejections ─► compare_with_etl

Layers (all in include/warehouse_hosur_narahari.duckdb):
  raw_*   exact copy of each CSV, all VARCHAR + _loaded_at/_source_file/_run_id/_row_num — append-only
  stg_*   SQL only (include/sql/stg_*.sql): cast, standardise, dedupe, mask PII, quality rules →
          dq_status / failure_reason; MERGE on the natural key so corrections overwrite, never duplicate.
          Rejected rows (as received) → stg_rejections; every check → dq_results
  mart_*  SQL only (include/sql/mart_*.sql): business-ready tables for the six questions

Trigger with config to load the next-day file:
  {"increment_file": "orders_2026-10-01_increment.csv"}
"""
import sys
from datetime import timedelta

import pendulum

sys.path.insert(0, "/usr/local/airflow/include")

from airflow.sdk import dag, task, get_current_context
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param

from nammamart import config as C
from nammamart import elt_steps as S

DAG_ID = f"nammamart_elt_{C.NAME}"

# ── Scheduling (Part C) ───────────────────────────────────────────────────────
# Client SLA: numbers ready before the 9:30 IST stand-up, Monday to Saturday.
#   SCHEDULE   = "0 9 * * 1-6"  → 09:00 every Mon–Sat, evaluated in the start_date's
#                timezone (Asia/Kolkata), i.e. 03:30 UTC. Airflow defaults to UTC, so a
#                naive start_date would silently fire at 09:00 UTC = 14:30 IST — after the meeting.
#   catchup    = False → if the laptop/Docker was off, Airflow runs ONE run for the latest
#                missed slot when it comes back, not one per missed day. The CSV exports are
#                full snapshots (orders.csv is the whole month) so a back-fill of every missed
#                9 AM would just reload the same files N times for no benefit.
#   retries    = 2 × 1 min → survives a transient DuckDB lock (the two DAGs share one file)
#                or a file that lands a minute late, while still finishing well before 9:30.
#   execution_timeout = 10 min per task → a hung task fails loudly instead of eating the SLA.
SCHEDULE = "0 9 * * 1-6"
START_DATE = pendulum.datetime(2026, 10, 1, tz=C.TZ)      # timezone-aware: Asia/Kolkata

default_args = {
    "owner": C.NAME,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}


def _run_id() -> str:
    return get_current_context()["run_id"]


def _param(name: str) -> str:
    return get_current_context()["params"][name]


@dag(
    dag_id=DAG_ID,
    description="NammaMart ELT: raw → staging → mart in DuckDB, transformations in SQL",
    schedule=SCHEDULE,                                        # 09:00 IST Mon–Sat (= 03:30 UTC)
    start_date=START_DATE,
    catchup=False,
    dagrun_timeout=timedelta(minutes=20),                     # whole run must finish well before 09:30
    default_args=default_args,
    max_active_runs=1,
    tags=["nammamart", "elt", C.NAME],
    params={
        "orders_file": Param(C.SOURCE_FILES["orders"], type="string"),
        "payments_file": Param(C.SOURCE_FILES["payments"], type="string"),
        "customers_file": Param(C.SOURCE_FILES["customers"], type="string"),
        "products_file": Param(C.SOURCE_FILES["products"], type="string"),
        "stores_file": Param(C.SOURCE_FILES["stores"], type="string"),
        "increment_file": Param(None, type=["null", "string"],
                                description="Optional next-day orders file to append to raw and MERGE into staging. "
                                            "Leave empty for a normal daily run."),
    },
)
def nammamart_elt():

    # ── EXTRACT + LOAD RAW (parallel, one task per file) ─────────────────────
    def _raw(entity):
        @task(task_id=f"raw_{entity}")
        def _t():
            return S.raw_load(entity, _param(f"{entity}_file"), _run_id())
        return _t()

    raw = {e: _raw(e) for e in ["customers", "products", "stores", "orders", "payments"]}

    # ── STAGING (SQL) ────────────────────────────────────────────────────────
    @task
    def stg_customers(r: dict, _orders_raw: dict):      # needs raw_orders for the first-order check
        return S.stage("customers", r["source_file"], _run_id())

    @task
    def stg_products(r: dict):
        return S.stage("products", r["source_file"], _run_id())

    @task
    def stg_stores(r: dict):
        return S.stage("stores", r["source_file"], _run_id())

    @task
    def stg_orders(r: dict, _c: dict, _p: dict, _s: dict):
        return S.stage("orders", r["source_file"], _run_id())

    @task
    def stg_payments(r: dict, _o: dict):
        return S.stage("payments", r["source_file"], _run_id())

    # ── INCREMENTAL LOAD: raw append + staging MERGE ─────────────────────────
    @task
    def raw_increment(_p: dict):
        inc = _param("increment_file")
        if not inc:
            raise AirflowSkipException("no increment_file configured for this run")
        return S.raw_load("orders", inc, _run_id())

    @task
    def stg_merge_increment(r: dict):
        return S.stage("orders", r["source_file"], _run_id(), label="orders_increment")

    # ── MART (SQL), sequential so there is one DuckDB writer at a time ───────
    def _mart(name):
        @task(task_id=name, trigger_rule="none_failed")   # runs even when the increment step is skipped
        def _t(*_):
            return S.build_mart(name, _run_id())
        return _t

    @task(trigger_rule="none_failed")
    def compare_with_etl(*_):
        return S.compare_with_etl()

    # ── WIRING ───────────────────────────────────────────────────────────────
    c = stg_customers(raw["customers"], raw["orders"])
    p = stg_products(raw["products"])
    s = stg_stores(raw["stores"])
    o = stg_orders(raw["orders"], c, p, s)
    pay = stg_payments(raw["payments"], o)
    inc_raw = raw_increment(pay)
    inc_stg = stg_merge_increment(inc_raw)

    prev = inc_stg
    for name in S.MARTS:
        prev = _mart(name)(prev)
    compare_with_etl(prev)


nammamart_elt()
