"""DuckDB helpers shared by both pipelines."""
from __future__ import annotations

import contextlib
import random
import time
from string import Template

import duckdb
import pandas as pd

from . import config as C

DQ_RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS dq_results (
    run_id        VARCHAR,
    dag_id        VARCHAR,
    check_name    VARCHAR,
    dimension     VARCHAR,
    severity      VARCHAR,
    rows_checked  BIGINT,
    rows_failed   BIGINT,
    status        VARCHAR,
    checked_at    TIMESTAMP
)"""

PIPELINE_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id           VARCHAR,
    dag_id           VARCHAR,
    start_time       TIMESTAMP,
    end_time         TIMESTAMP,
    rows_extracted   BIGINT,
    rows_loaded      BIGINT,
    rows_quarantined BIGINT,
    status           VARCHAR
)"""


@contextlib.contextmanager
def connect(read_only: bool = False, retries: int = 40, wait: float = 1.5):
    """DuckDB allows ONE writer process per file. Parallel Airflow tasks (and the
    ETL + ELT DAGs) share this file, so wait politely for the lock instead of failing.
    40 × ~1.5 s ≈ 60 s of patience, which covers any task in these pipelines."""
    C.WAREHOUSE_DB.parent.mkdir(parents=True, exist_ok=True)
    last = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(str(C.WAREHOUSE_DB), read_only=read_only)
            break
        except duckdb.IOException as e:  # lock held by another process
            last = e
            if attempt == 0:
                print(f"[warehouse] lock busy, waiting… ({e})")
            time.sleep(wait + random.random())
    else:
        raise last
    try:
        yield con
    finally:
        con.close()


def replace_table(con, table: str, df: pd.DataFrame):
    con.register("_df", df)
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM _df")
    con.unregister("_df")


def upsert(con, table: str, df: pd.DataFrame, key: str) -> dict:
    """Idempotent load: delete rows whose key is in the batch, then insert the batch.
    Returns counts so the caller can report inserted vs updated."""
    con.register("_df", df)
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM _df WHERE 1=0")
    before = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    updated = con.execute(f"SELECT COUNT(*) FROM {table} WHERE {key} IN (SELECT {key} FROM _df)").fetchone()[0]
    con.execute(f"DELETE FROM {table} WHERE {key} IN (SELECT {key} FROM _df)")
    con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM _df")
    after = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    con.unregister("_df")
    return {"before": before, "updated": updated, "inserted": len(df) - updated, "after": after}


ORDER_UPSERT_GUARD = """
    NOT (t.order_status IN ('RETURNED', 'CANCELLED') AND s.order_status NOT IN ('RETURNED', 'CANCELLED'))
    AND coalesce(s.effective_date >= t.effective_date, TRUE)"""


def upsert_guarded(con, table: str, df: pd.DataFrame, key: str, guard: str) -> dict:
    """Like upsert(), but an existing row (alias t) is only replaced by the incoming row
    (alias s) when `guard` is TRUE. Used so a stale full snapshot can never undo a later
    correction (e.g. RETURNED back to DELIVERED). Returns inserted/updated/skipped counts."""
    con.register("_df", df)
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM _df WHERE 1=0")
    before = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    skipped = con.execute(f"SELECT COUNT(*) FROM {table} t JOIN _df s USING ({key}) WHERE NOT ({guard})").fetchone()[0]
    updated = con.execute(f"SELECT COUNT(*) FROM {table} t JOIN _df s USING ({key}) WHERE ({guard})").fetchone()[0]
    con.execute(f"""DELETE FROM {table} WHERE {key} IN (
                    SELECT t.{key} FROM {table} t JOIN _df s USING ({key}) WHERE ({guard}))""")
    con.execute(f"""INSERT INTO {table} BY NAME
                    SELECT * FROM _df s WHERE s.{key} NOT IN (SELECT {key} FROM {table})""")
    after = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    con.unregister("_df")
    return {"before": before, "updated": updated, "inserted": len(df) - updated - skipped,
            "skipped_guarded": skipped, "after": after}


def write_dq_results(con, run_id: str, dag_id: str, results: list[dict], checked_at):
    con.execute(DQ_RESULTS_DDL)
    # idempotent per run: a re-run of the same run_id (task Clear) replaces its rows
    con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
    df = pd.DataFrame(results)
    if df.empty:
        return 0
    df.insert(0, "dag_id", dag_id)
    df.insert(0, "run_id", run_id)
    df["checked_at"] = pd.Timestamp(checked_at)
    con.register("_dq", df[["run_id", "dag_id", "check_name", "dimension", "severity",
                            "rows_checked", "rows_failed", "status", "checked_at"]])
    con.execute("INSERT INTO dq_results SELECT * FROM _dq")
    con.unregister("_dq")
    return len(df)


def write_pipeline_run(con, row: dict):
    con.execute(PIPELINE_RUNS_DDL)
    con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [row["run_id"], row["dag_id"]])
    con.execute(
        "INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?)",
        [row["run_id"], row["dag_id"], row["start_time"], row["end_time"],
         row["rows_extracted"], row["rows_loaded"], row["rows_quarantined"], row["status"]],
    )


def run_sql_file(con, name: str, **params) -> str:
    """Render include/sql/<name>.sql with $param placeholders and execute it (multi-statement)."""
    path = C.SQL_DIR / (name if name.endswith(".sql") else f"{name}.sql")
    sql = Template(path.read_text()).safe_substitute(**params)
    con.execute(sql)
    return sql
