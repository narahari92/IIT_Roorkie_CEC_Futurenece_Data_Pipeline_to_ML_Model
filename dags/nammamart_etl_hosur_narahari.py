"""
dags/nammamart_etl_hosur_narahari.py
------------------------------------
NammaMart ETL pipeline — Extract → Validate → Quarantine → Transform → Load → Reconcile → Report

  extract_customers ─► validate_customers ─┐
  extract_products  ─► validate_products  ─┼─► validate_orders ─► validate_payments ─► quarantine_summary
  extract_stores    ─► validate_stores    ─┤          │                  │                     │
  extract_orders    ─────────────────────-─┘          ▼                  ▼                     ▼
  extract_payments  ───────────────────────────► transform ─────────► load ─► reconcile ─► report
                                                                              └─► write_dq_results

Rules of the road
  * Bad rows never reach the finance tables: every Critical failure is written to
    include/quarantine/hosur_narahari/<file>_failed_<run_id>.csv (and the etl_quarantine table)
    with a failure_reason. Warning failures load, but are logged in dq_results.
  * Loading is idempotent: dims are fully replaced, the fact table is upserted on order_id,
    so re-running (or loading the next-day increment) never double counts.
  * All logic lives in include/nammamart/ so it can be unit-tested outside Airflow.

Trigger with config to point at other files (used for the increment and the Part F experiments):
  {"orders_file": "orders_2026-10-01_increment.csv"}
"""
import sys
from datetime import timedelta

import pendulum

sys.path.insert(0, "/usr/local/airflow/include")

from airflow.sdk import dag, task, get_current_context          # Airflow 3 public SDK
from airflow.models.param import Param

from nammamart import config as C
from nammamart import etl_steps as S
from nammamart import warehouse as W

DAG_ID = f"nammamart_etl_{C.NAME}"

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
    description="NammaMart ETL: validate in Python, quarantine bad rows, load a star schema to DuckDB",
    schedule=None,                                            # ← set to 9:00 IST Mon–Sat in Part C
    start_date=pendulum.datetime(2026, 10, 1, tz=C.TZ),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,                                        # one writer to the DuckDB file at a time
    tags=["nammamart", "etl", C.NAME],
    params={
        "orders_file": Param(C.SOURCE_FILES["orders"], type="string"),
        "payments_file": Param(C.SOURCE_FILES["payments"], type="string"),
        "customers_file": Param(C.SOURCE_FILES["customers"], type="string"),
        "products_file": Param(C.SOURCE_FILES["products"], type="string"),
        "stores_file": Param(C.SOURCE_FILES["stores"], type="string"),
    },
)
def nammamart_etl():

    # ── EXTRACT: five parallel tasks ─────────────────────────────────────────
    @task
    def extract_orders():
        return S.extract("orders", _param("orders_file"), _run_id())

    @task
    def extract_payments():
        return S.extract("payments", _param("payments_file"), _run_id())

    @task
    def extract_customers():
        return S.extract("customers", _param("customers_file"), _run_id())

    @task
    def extract_products():
        return S.extract("products", _param("products_file"), _run_id())

    @task
    def extract_stores():
        return S.extract("stores", _param("stores_file"), _run_id())

    # ── VALIDATE + QUARANTINE: dims first (parallel), then orders, then payments ──
    @task
    def validate_customers(ex: dict, orders_ex: dict):
        return S.validate("customers", ex, _run_id(), S.customers_context(orders_ex))

    @task
    def validate_products(ex: dict):
        return S.validate("products", ex, _run_id())

    @task
    def validate_stores(ex: dict):
        return S.validate("stores", ex, _run_id())

    @task
    def validate_orders(ex: dict, cust: dict, prod: dict, store: dict):
        return S.validate("orders", ex, _run_id(), S.orders_context(cust, prod, store))

    @task
    def validate_payments(ex: dict, orders_ex: dict, orders_v: dict):
        return S.validate("payments", ex, _run_id(), S.payments_context(orders_ex, orders_v))

    @task
    def quarantine_summary(validated: list):
        return S.quarantine_summary(validated, _run_id())

    @task
    def write_dq_results(validated: list):
        results = [r for v in validated for r in v["dq_results"]]
        with W.connect() as con:
            n = W.write_dq_results(con, _run_id(), DAG_ID, results, pendulum.now(C.TZ).naive())
            scorecard = con.execute(
                "SELECT dimension, severity, status, COUNT(*) AS checks, SUM(rows_failed) AS rows_failed "
                "FROM dq_results WHERE run_id = ? AND dag_id = ? GROUP BY ALL ORDER BY 1,2,3",
                [_run_id(), DAG_ID]).fetchdf()
        print(f"[DQ] {n} check results written to dq_results\n{scorecard.to_string(index=False)}")
        return n

    # ── TRANSFORM / LOAD / RECONCILE / REPORT ────────────────────────────────
    @task
    def transform(orders_v: dict, cust: dict, prod: dict, store: dict):
        return S.transform(orders_v, cust, prod, store, _run_id())

    @task
    def load(transformed: dict, payments_v: dict, validated: list):
        return S.load(transformed, payments_v, _run_id(), validated)

    @task
    def reconcile(_loaded: dict):
        return S.reconcile(_run_id())

    @task
    def report(_reconciled: dict):
        return S.report(_run_id())

    # ── WIRING ───────────────────────────────────────────────────────────────
    ex_orders = extract_orders()
    ex_payments = extract_payments()
    ex_customers = extract_customers()
    ex_products = extract_products()
    ex_stores = extract_stores()

    v_customers = validate_customers(ex_customers, ex_orders)
    v_products = validate_products(ex_products)
    v_stores = validate_stores(ex_stores)
    v_orders = validate_orders(ex_orders, v_customers, v_products, v_stores)
    v_payments = validate_payments(ex_payments, ex_orders, v_orders)

    validated = [v_customers, v_products, v_stores, v_orders, v_payments]
    quarantine_summary(validated)
    write_dq_results(validated)

    transformed = transform(v_orders, v_customers, v_products, v_stores)
    loaded = load(transformed, v_payments, validated)
    reconciled = reconcile(loaded)
    report(reconciled)


nammamart_etl()
