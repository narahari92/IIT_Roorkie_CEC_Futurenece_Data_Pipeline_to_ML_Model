"""
Step functions for the NammaMart ETL DAG. Each function is plain Python so it
can be unit-tested or run locally without Airflow:

    AIRFLOW_HOME=$PWD .venv/bin/python -m include.nammamart.etl_steps

Data between steps is passed as CSV files in WORK_DIR/<run_id>/ (small,
inspectable, and keeps XCom payloads tiny).
"""
from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

import pandas as pd
import pendulum

from . import config as C
from . import quality as Q
from .masking import mask_customers
from .standardise import STANDARDISERS
from . import warehouse as W

DAG_ID = f"nammamart_etl_{C.NAME}"


def _work(run_id: str) -> Path:
    d = C.WORK_DIR / C.safe_run_id(run_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def _read(path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False).replace({"": pd.NA})


def _now():
    return pendulum.now(C.TZ)


# ───────────────────────────── EXTRACT ─────────────────────────────
def extract(entity: str, file_name: str, run_id: str) -> dict:
    """Read one CSV exactly as received (all text) and snapshot it to the work dir."""
    src = C.DATA_DIR / file_name
    if not src.exists():
        raise FileNotFoundError(f"[Extract:{entity}] source file not found: {src}")
    df = pd.read_csv(src, dtype=str, keep_default_na=False)
    expected_cols = _expected_columns(entity)
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"[Extract:{entity}] schema check failed — missing columns {missing}; got {list(df.columns)}")
    out = _work(run_id) / f"{entity}_raw.csv"
    df.to_csv(out, index=False)
    print(f"[Extract:{entity}] {len(df)} rows, {df.shape[1]} cols from {src.name} → {out.name}")
    return {"entity": entity, "source_file": file_name, "path": str(out), "rows": len(df)}


def _expected_columns(entity):
    return {
        "orders": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price",
                   "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"],
        "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status", "gateway_ref"],
        "customers": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode", "signup_date", "loyalty_tier", "is_active"],
        "products": ["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit", "is_perishable", "launch_date"],
        "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name", "capacity_orders_per_day"],
    }[entity]


# ───────────────────────────── VALIDATE ─────────────────────────────
def validate(entity: str, extracted: dict, run_id: str, ctx: dict | None = None) -> dict:
    """Standardise → run quality rules → split into passed / failed.
    Failed rows go straight to the quarantine folder with a failure_reason."""
    raw = _read(extracted["path"])
    df = STANDARDISERS[entity](raw)
    ctx = dict(ctx or {})
    ctx.setdefault("now", pd.Timestamp(_now().naive()))
    ctx["raw"] = raw

    passed, failed, results = Q.run_checks(df, Q.CHECKS[entity], ctx)

    work = _work(run_id)
    passed_path = work / f"{entity}_passed.csv"
    passed.to_csv(passed_path, index=False)

    C.QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    q_path = C.QUARANTINE_DIR / f"{Path(extracted['source_file']).stem}_failed_{C.safe_run_id(run_id)}.csv"
    # quarantine the row AS RECEIVED (raw values), plus the reason
    q_df = raw.loc[failed.index].copy()
    q_df["failure_reason"] = failed["failure_reason"].values
    q_df["dq_warnings"] = failed["dq_warnings"].values
    q_df["quarantined_at"] = _now().to_iso8601_string()
    q_df["run_id"] = run_id
    q_df.to_csv(q_path, index=False)

    print(f"[Validate:{entity}] in={len(df)} passed={len(passed)} quarantined={len(failed)} → {q_path.name}")
    print(Q.summarise(results))
    if len(failed):
        print(f"[Validate:{entity}] quarantine reasons:\n"
              + failed["failure_reason"].str.split(";").explode().value_counts().to_string())

    return {
        "entity": entity, "source_file": extracted["source_file"], "raw_path": extracted["path"],
        "rows_in": len(df), "rows_passed": len(passed), "rows_failed": len(failed),
        "passed_path": str(passed_path), "quarantine_path": str(q_path),
        "dq_results": [r.as_dict() for r in results],
    }


def orders_context(cust: dict, prod: dict, store: dict) -> dict:
    customers = _read(cust["passed_path"])
    products = _read(prod["passed_path"])
    stores = _read(store["passed_path"])
    return {
        "customer_ids": set(customers["customer_id"].dropna()),
        "product_ids": set(products["product_id"].dropna()),
        "store_ids": set(stores["store_id"].dropna()),
        "customer_ids_raw": set(_read(cust["raw_path"])["customer_id"].dropna().str.strip()),
        "product_ids_raw": set(_read(prod["raw_path"])["product_id"].dropna().str.strip()),
        "store_ids_raw": set(_read(store["raw_path"])["store_id"].dropna().str.strip()),
        "product_mrp": products.drop_duplicates("product_id", keep="first").set_index("product_id")["mrp"].to_dict(),
    }


def payments_context(orders_extracted: dict, orders_validated: dict) -> dict:
    """Payments may refer to orders from earlier runs (e.g. when today's orders
    file is only the increment), so 'known orders' = this run's raw file ∪ what the
    warehouse already holds (clean fact rows and previously quarantined orders)."""
    raw_orders = _read(orders_extracted["path"])
    clean = _read(orders_validated["passed_path"])
    known = set(raw_orders["order_id"].dropna().str.strip())
    mode = clean.set_index("order_id")["payment_mode"].to_dict()
    amount = clean.set_index("order_id")["order_amount"].to_dict()
    if C.WAREHOUSE_DB.exists():
        with W.connect(read_only=True) as con:
            tables = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
            if "etl_fact_orders" in tables:
                hist = con.execute("SELECT order_id, payment_mode, order_amount FROM etl_fact_orders").fetchdf()
                known |= set(hist["order_id"])
                for r in hist.itertuples(index=False):
                    mode.setdefault(r.order_id, r.payment_mode)
                    amount.setdefault(r.order_id, str(r.order_amount))
            if "etl_quarantine" in tables:
                known |= set(con.execute("SELECT DISTINCT row_key FROM etl_quarantine WHERE entity='orders'").fetchdf()["row_key"])
    return {"order_ids_raw": known, "order_mode": mode, "order_amount": amount}


def customers_context(orders_extracted: dict) -> dict:
    raw_orders = _read(orders_extracted["path"])
    first = pd.to_datetime(raw_orders["order_ts"], errors="coerce", format=Q.TS_FORMAT).groupby(raw_orders["customer_id"]).min()
    return {"first_order_ts": first.to_dict()}


# ───────────────────────────── QUARANTINE SUMMARY ─────────────────────────────
def quarantine_summary(validated: list[dict], run_id: str) -> dict:
    summary = {}
    total = 0
    for v in validated:
        reasons = {}
        if v["rows_failed"]:
            q = pd.read_csv(v["quarantine_path"], dtype=str)
            reasons = q["failure_reason"].str.split(";").explode().value_counts().to_dict()
        summary[v["source_file"]] = {"rows_in": v["rows_in"], "rows_passed": v["rows_passed"],
                                     "rows_quarantined": v["rows_failed"], "by_reason": reasons,
                                     "file": v["quarantine_path"]}
        total += v["rows_failed"]
    path = C.QUARANTINE_DIR / f"summary_{C.safe_run_id(run_id)}.json"
    path.write_text(json.dumps({"run_id": run_id, "total_quarantined": total, "files": summary}, indent=2, default=str))
    print(f"[Quarantine] {total} rows set aside across {len(validated)} files → {path.name}")
    for f, s in summary.items():
        print(f"  {f:40} in={s['rows_in']:>5} passed={s['rows_passed']:>5} quarantined={s['rows_quarantined']:>4}")
        for r, n in s["by_reason"].items():
            print(f"      - {r}: {n}")
    return {"total_quarantined": total, "summary_path": str(path)}


# ───────────────────────────── TRANSFORM ─────────────────────────────
def transform(orders_v: dict, cust_v: dict, prod_v: dict, store_v: dict, run_id: str) -> dict:
    orders = _read(orders_v["passed_path"])
    customers = _read(cust_v["passed_path"])
    products = _read(prod_v["passed_path"])
    stores = _read(store_v["passed_path"])
    work = _work(run_id)

    # ── dimensions: dedupe (uniqueness rules already removed superseded rows), cast, mask ──
    customers = customers.drop_duplicates("customer_id", keep="last")
    customers = mask_customers(customers)
    customers["signup_date"] = pd.to_datetime(customers["signup_date"], errors="coerce").dt.date
    customers["is_active"] = customers["is_active"].eq("Y")
    dim_customer = customers[["customer_id", "full_name", "email", "phone", "city", "area", "pincode",
                              "signup_date", "loyalty_tier", "is_active"]].rename(
        columns={"email": "email_masked", "phone": "phone_masked"})

    products = products.drop_duplicates("product_id", keep="first")
    products["category"] = products["category"].fillna("Unknown")
    for c in ["mrp", "cost_price"]:
        products[c] = pd.to_numeric(products[c])
    products["is_perishable"] = products["is_perishable"].eq("True")
    products["launch_date"] = pd.to_datetime(products["launch_date"], errors="coerce").dt.date
    dim_product = products[["product_id", "product_name", "category", "sub_category", "brand", "mrp",
                            "cost_price", "unit", "is_perishable", "launch_date"]]

    stores = stores.drop_duplicates("store_id", keep="first")
    stores["zone"] = stores["zone"].fillna("Unknown")
    stores["capacity_orders_per_day"] = pd.to_numeric(stores["capacity_orders_per_day"]).astype("Int64")
    stores["open_date"] = pd.to_datetime(stores["open_date"], errors="coerce").dt.date
    dim_store = stores[["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name",
                        "capacity_orders_per_day"]]

    # ── fact: cast, join, derive ──
    o = orders.copy()
    o["order_ts"] = pd.to_datetime(o["order_ts"], format=Q.TS_FORMAT)
    o["order_date"] = o["order_ts"].dt.date
    for c in ["quantity", "delivery_minutes"]:
        o[c] = pd.to_numeric(o[c]).astype("Int64")
    for c in ["unit_price", "discount_pct", "order_amount"]:
        o[c] = pd.to_numeric(o[c])

    o = o.merge(dim_product[["product_id", "cost_price", "category"]], on="product_id", how="left", validate="m:1")
    o = o.merge(dim_store[["store_id", "zone"]], on="store_id", how="left", validate="m:1")
    o = o.merge(dim_customer[["customer_id", "loyalty_tier"]], on="customer_id", how="left", validate="m:1")

    delivered = o["order_status"].eq("DELIVERED")
    o["is_delivered"] = delivered
    o["net_revenue"] = o["order_amount"].where(delivered, 0.0).round(2)
    o["cogs"] = (o["quantity"].astype(float) * o["cost_price"]).where(delivered, 0.0).round(2)
    o["gross_margin"] = (o["net_revenue"] - o["cogs"]).round(2)
    o["within_15_min"] = (o["delivery_minutes"] <= C.SLA_MINUTES).where(delivered & o["delivery_minutes"].notna())
    o["loaded_at"] = pd.Timestamp(_now().naive())
    o["run_id"] = run_id
    o["effective_date"] = pd.Series([effective_date(orders_v["source_file"])] * len(o), index=o.index, dtype="object")
    o["effective_date"] = pd.to_datetime(o["effective_date"]).dt.date

    fact = o[["order_id", "order_ts", "order_date", "customer_id", "store_id", "product_id", "quantity", "unit_price",
              "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes",
              "is_delivered", "net_revenue", "cogs", "gross_margin", "within_15_min", "dq_warnings", "loaded_at", "run_id",
              "effective_date"]]

    paths = {}
    for name, df in [("fact_orders", fact), ("dim_customer", dim_customer), ("dim_product", dim_product), ("dim_store", dim_store)]:
        p = work / f"{name}.parquet"
        df.to_parquet(p, index=False)
        paths[name] = str(p)
        print(f"[Transform] {name}: {len(df)} rows")
    print(f"[Transform] fact totals — delivered={int(delivered.sum())} net_revenue={fact['net_revenue'].sum():,.2f} "
          f"gross_margin={fact['gross_margin'].sum():,.2f}")
    return {"paths": paths, "fact_rows": len(fact)}


def effective_date(source_file: str, _order_ts: pd.Series = None):
    """'As-of' date of a batch = the date in the file name (orders_2026-10-01_increment.csv →
    2026-10-01), else None. Deliberately NOT derived from the data: junk future timestamps
    would make a stale snapshot look newer than a real correction."""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", source_file)
    return pd.Timestamp(m.group(1)).date() if m else None


# ───────────────────────────── LOAD ─────────────────────────────
def _quarantine_table(validated: list[dict], run_id: str) -> pd.DataFrame:
    """One long table of every quarantined row this run: file, key, reason, and the raw row as JSON."""
    frames = []
    for v in validated:
        if not v["rows_failed"]:
            continue
        q = pd.read_csv(v["quarantine_path"], dtype=str, keep_default_na=False)
        key = C.PRIMARY_KEYS[v["entity"]]
        meta = ["failure_reason", "dq_warnings", "quarantined_at", "run_id"]
        frames.append(pd.DataFrame({
            "run_id": run_id,
            "source_file": v["source_file"],
            "entity": v["entity"],
            "row_key": q[key],
            "failure_reason": q["failure_reason"],
            "dq_warnings": q["dq_warnings"].replace("", None),
            "raw_row": q.drop(columns=meta).apply(lambda r: json.dumps(r.to_dict()), axis=1),
            "quarantined_at": pd.to_datetime(q["quarantined_at"]).dt.tz_localize(None),
        }))
    cols = ["run_id", "source_file", "entity", "row_key", "failure_reason", "dq_warnings", "raw_row", "quarantined_at"]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=cols)


def load(transformed: dict, payments_v: dict, run_id: str, validated: list[dict] | None = None) -> dict:
    fact = pd.read_parquet(transformed["paths"]["fact_orders"])
    payments = _read(payments_v["passed_path"])
    payments["payment_ts"] = pd.to_datetime(payments["payment_ts"], format=Q.TS_FORMAT)
    payments["amount_paid"] = pd.to_numeric(payments["amount_paid"])
    payments["run_id"] = run_id

    with W.connect() as con:
        for dim in ["dim_customer", "dim_product", "dim_store"]:
            W.replace_table(con, f"etl_{dim}", pd.read_parquet(transformed["paths"][dim]))
        fact_stats = W.upsert_guarded(con, "etl_fact_orders", fact, "order_id", W.ORDER_UPSERT_GUARD)
        pay_stats = W.upsert(con, "etl_payments", payments, "payment_id")
        if validated is not None:
            q = _quarantine_table(validated, run_id)
            con.register("_q", q)
            con.execute("CREATE TABLE IF NOT EXISTS etl_quarantine AS SELECT * FROM _q WHERE 1=0")
            con.execute("DELETE FROM etl_quarantine WHERE run_id = ?", [run_id])   # idempotent per run
            con.execute("INSERT INTO etl_quarantine BY NAME SELECT * FROM _q")
            con.unregister("_q")
            print(f"[Load] etl_quarantine: {len(q)} rows for this run")
        counts = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                  for t in ["etl_fact_orders", "etl_dim_customer", "etl_dim_product", "etl_dim_store", "etl_payments", "etl_quarantine"]}
    print(f"[Load] etl_fact_orders: before={fact_stats['before']} updated={fact_stats['updated']} "
          f"inserted={fact_stats['inserted']} skipped_guarded={fact_stats['skipped_guarded']} after={fact_stats['after']} "
          f"(batch effective_date={fact['effective_date'].iloc[0] if len(fact) else None})")
    print(f"[Load] etl_payments:    before={pay_stats['before']} updated={pay_stats['updated']} "
          f"inserted={pay_stats['inserted']} after={pay_stats['after']}")
    print(f"[Load] table counts: {counts}")
    return {"fact": fact_stats, "payments": pay_stats, "counts": counts, "rows_loaded": len(fact)}


# ───────────────────────────── RECONCILE ─────────────────────────────
def reconcile(run_id: str) -> dict:
    with W.connect() as con:
        fact = con.execute("SELECT order_id, order_status, order_amount, payment_mode FROM etl_fact_orders").fetchdf()
        pay = con.execute("SELECT payment_id, order_id, payment_status, amount_paid, payment_mode FROM etl_payments").fetchdf()
        quarantined_orders = set(con.execute(
            "SELECT DISTINCT row_key FROM etl_quarantine WHERE entity = 'orders'").fetchdf()["row_key"]) \
            if con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name='etl_quarantine'").fetchone()[0] else set()

    rows = []
    merged = fact.merge(pay, on="order_id", how="outer", suffixes=("", "_pay"), indicator=True)
    for _, r in merged.iterrows():
        reason = None
        if r["_merge"] == "right_only":
            reason = "payment_for_quarantined_order" if r["order_id"] in quarantined_orders else "payment_without_order"
        elif r["_merge"] == "left_only":
            if r["order_status"] == "DELIVERED":
                reason = "delivered_without_payment"
            elif r["order_status"] == "RETURNED":
                reason = "returned_without_refund"
        else:
            if r["order_status"] == "DELIVERED" and r["payment_status"] == "FAILED":
                reason = "delivered_but_payment_failed"
            elif r["order_status"] == "DELIVERED" and r["payment_status"] == "REFUNDED":
                reason = "delivered_but_payment_refunded"
            elif r["order_status"] == "CANCELLED" and r["payment_status"] == "SUCCESS":
                reason = "cancelled_but_payment_success"
            elif r["order_status"] == "RETURNED" and r["payment_status"] == "SUCCESS":
                reason = "returned_but_not_refunded"
            elif r["payment_status"] == "SUCCESS" and abs(float(r["amount_paid"]) - float(r["order_amount"])) > 0.01:
                reason = "amount_mismatch"
            elif pd.notna(r["payment_mode_pay"]) and r["payment_mode_pay"] != r["payment_mode"]:
                reason = "payment_mode_mismatch"
        if reason:
            rows.append({
                "order_id": r["order_id"], "payment_id": r.get("payment_id"),
                "order_status": r.get("order_status"), "order_amount": r.get("order_amount"),
                "payment_status": r.get("payment_status"), "amount_paid": r.get("amount_paid"),
                "delta": (float(r["amount_paid"]) if pd.notna(r.get("amount_paid")) else 0.0)
                         - (float(r["order_amount"]) if pd.notna(r.get("order_amount")) else 0.0),
                "reason": reason,
            })
    recon = pd.DataFrame(rows, columns=["order_id", "payment_id", "order_status", "order_amount", "payment_status",
                                        "amount_paid", "delta", "reason"])
    recon["run_id"] = run_id
    recon["reconciled_at"] = pd.Timestamp(_now().naive())
    with W.connect() as con:
        W.replace_table(con, "etl_payment_reconciliation", recon)
    by_reason = recon.groupby("reason").agg(mismatches=("order_id", "size"), delta=("delta", "sum")) if len(recon) else pd.DataFrame()
    print(f"[Reconcile] {len(recon)} mismatches written to etl_payment_reconciliation")
    print(by_reason.to_string() if len(recon) else "  none")
    return {"mismatches": len(recon), "by_reason": {k: int(v) for k, v in recon["reason"].value_counts().items()}}


# ───────────────────────────── REPORT ─────────────────────────────
REPORT_SQL = {
    "etl_daily_zone_revenue": """
        CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
        SELECT f.order_date, s.zone, f.store_id, s.store_name,
               COUNT(*) FILTER (WHERE f.is_delivered)       AS delivered_orders,
               ROUND(SUM(f.net_revenue), 2)                 AS net_revenue,
               ROUND(SUM(f.gross_margin), 2)                AS gross_margin
        FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
        GROUP BY ALL ORDER BY f.order_date, s.zone, f.store_id""",
    "etl_category_margin": """
        CREATE OR REPLACE TABLE etl_category_margin AS
        SELECT p.category,
               COUNT(*) FILTER (WHERE f.is_delivered)  AS delivered_orders,
               SUM(f.quantity) FILTER (WHERE f.is_delivered) AS units,
               ROUND(SUM(f.net_revenue), 2)            AS revenue,
               ROUND(SUM(f.cogs), 2)                   AS cogs,
               ROUND(SUM(f.gross_margin), 2)           AS gross_margin,
               ROUND(100 * SUM(f.gross_margin) / NULLIF(SUM(f.net_revenue), 0), 2) AS margin_pct
        FROM etl_fact_orders f JOIN etl_dim_product p USING (product_id)
        GROUP BY ALL ORDER BY revenue DESC""",
}


def report(run_id: str) -> dict:
    out = {}
    with W.connect() as con:
        for table, sql in REPORT_SQL.items():
            con.execute(sql)
            df = con.execute(f"SELECT * FROM {table}").fetchdf()
            out[table] = len(df)
            print(f"\n[Report] {table} ({len(df)} rows)")
            if table == "etl_daily_zone_revenue":
                zone = con.execute("""SELECT zone, SUM(delivered_orders) delivered_orders, ROUND(SUM(net_revenue),2) net_revenue
                                      FROM etl_daily_zone_revenue GROUP BY zone ORDER BY net_revenue DESC""").fetchdf()
                print(zone.to_string(index=False))
                print("  (first 10 daily rows)")
                print(df.head(10).to_string(index=False))
            else:
                print(df.to_string(index=False))
        total = con.execute("SELECT ROUND(SUM(net_revenue),2), COUNT(*) FILTER (WHERE is_delivered) FROM etl_fact_orders").fetchone()
    print(f"\n[Report] TOTAL net revenue = {total[0]:,.2f} from {total[1]} delivered orders")
    out["total_net_revenue"] = float(total[0])
    return out


# ───────────────────────────── local driver ─────────────────────────────
if __name__ == "__main__":
    run_id = "local__" + _now().format("YYYYMMDD_HHmmss")
    ex = {e: extract(e, f, run_id) for e, f in C.SOURCE_FILES.items()}
    v_cust = validate("customers", ex["customers"], run_id, customers_context(ex["orders"]))
    v_prod = validate("products", ex["products"], run_id)
    v_store = validate("stores", ex["stores"], run_id)
    v_ord = validate("orders", ex["orders"], run_id, orders_context(v_cust, v_prod, v_store))
    v_pay = validate("payments", ex["payments"], run_id, payments_context(ex["orders"], v_ord))
    quarantine_summary([v_cust, v_prod, v_store, v_ord, v_pay], run_id)
    t = transform(v_ord, v_cust, v_prod, v_store, run_id)
    load(t, v_pay, run_id, [v_cust, v_prod, v_store, v_ord, v_pay])
    reconcile(run_id)
    report(run_id)
    with W.connect() as con:
        all_results = sum((v["dq_results"] for v in [v_cust, v_prod, v_store, v_ord, v_pay]), [])
        W.write_dq_results(con, run_id, DAG_ID, all_results, _now().naive())
        print(con.execute("SELECT status, COUNT(*) FROM dq_results WHERE run_id = ? GROUP BY 1", [run_id]).fetchdf())
