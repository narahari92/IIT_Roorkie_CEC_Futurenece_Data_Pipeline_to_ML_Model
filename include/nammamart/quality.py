"""
Data-quality rules for the ETL pipeline (Python). The ELT pipeline applies the
same rules in SQL (include/sql/stg_*.sql) so the two pipelines agree.

Every rule is a `Check`: a name, one of the six quality dimensions, a severity
and a function that returns a boolean mask of FAILING rows (or, for
dataset-level checks, an integer count of failures).

  Critical → row goes to quarantine with failure_reason
  Warning  → row is loaded, reason is kept in `dq_warnings`, counted in dq_results
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable

import pandas as pd

from . import config as C

CRITICAL = "Critical"
WARNING = "Warning"
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"
EMAIL_RE = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
PHONE_RE = r"^[6-9]\d{9}$"
PINCODE_RE = r"^[1-9]\d{5}$"


@dataclass
class Check:
    name: str
    dimension: str
    severity: str
    fn: Callable[[pd.DataFrame, dict], "pd.Series | int"]
    row_level: bool = True
    description: str = ""


@dataclass
class CheckResult:
    check_name: str
    dimension: str
    severity: str
    rows_checked: int
    rows_failed: int
    status: str  # PASS / WARN / FAIL

    def as_dict(self):
        return self.__dict__.copy()


# ─────────────────────────── helpers ───────────────────────────
def num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def ts(s: pd.Series, fmt=TS_FORMAT) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", format=fmt)


def is_blank(s: pd.Series) -> pd.Series:
    return s.isna() | (s.astype("string").str.strip() == "")


def not_in(s: pd.Series, allowed) -> pd.Series:
    return s.notna() & ~s.isin(allowed)


def future(s: pd.Series, ctx, fmt=TS_FORMAT) -> pd.Series:
    parsed = ts(s, fmt)
    return parsed.notna() & (parsed > ctx["now"])


def not_in_master(s: pd.Series, key: str, ctx) -> pd.Series:
    """id is not in the master file at all (checked against the RAW master)."""
    return s.notna() & ~s.isin(ctx.get(f"{key}_raw", ctx.get(key, set())))


def master_quarantined(s: pd.Series, key: str, ctx) -> pd.Series:
    """id exists in the raw master but that master row failed a Critical rule."""
    raw, clean = ctx.get(f"{key}_raw"), ctx.get(key)
    if raw is None or clean is None:
        return pd.Series(False, index=s.index)
    return s.notna() & s.isin(raw) & ~s.isin(clean)


def stale_dataset(col, fmt=TS_FORMAT):
    """Dataset-level freshness: fail (1) if the newest timestamp is older than STALE_AFTER_DAYS."""
    def _fn(df, ctx):
        parsed = ts(df[col], fmt).dropna()
        parsed = parsed[parsed <= ctx["now"]]      # future-dated junk must not hide staleness
        if parsed.empty:
            return 1
        age_days = (ctx["now"] - parsed.max()).total_seconds() / 86400
        return 1 if age_days > C.STALE_AFTER_DAYS else 0
    return _fn


def low_volume(min_rows):
    def _fn(df, ctx):
        return 1 if len(df) < min_rows else 0
    return _fn


# ─────────────────────────── ORDERS ───────────────────────────
def _amount_mismatch(df, ctx):
    q, up, d, amt = num(df["quantity"]), num(df["unit_price"]), num(df["discount_pct"]), num(df["order_amount"])
    expected = (q * up * (1 - d / 100)).round(2)
    return expected.notna() & amt.notna() & ((expected - amt).abs() > 0.01)


ORDER_CHECKS = [
    # Completeness
    Check("orders.order_id_not_null", "Completeness", CRITICAL, lambda df, c: is_blank(df["order_id"])),
    Check("orders.customer_id_not_null", "Completeness", CRITICAL, lambda df, c: is_blank(df["customer_id"])),
    Check("orders.order_ts_not_null", "Completeness", CRITICAL, lambda df, c: is_blank(df["order_ts"])),
    Check("orders.delivered_has_delivery_minutes", "Completeness", CRITICAL,
          lambda df, c: (df["order_status"] == "DELIVERED") & num(df["delivery_minutes"]).isna()),
    # Uniqueness
    Check("orders.order_id_unique", "Uniqueness", CRITICAL, lambda df, c: df["order_id"].duplicated(keep="first")),
    # Validity
    Check("orders.order_ts_parses", "Validity", CRITICAL, lambda df, c: df["order_ts"].notna() & ts(df["order_ts"]).isna()),
    Check("orders.order_status_in_list", "Validity", CRITICAL, lambda df, c: not_in(df["order_status"], C.VALID_ORDER_STATUS)),
    Check("orders.payment_mode_in_list", "Validity", CRITICAL, lambda df, c: not_in(df["payment_mode"], C.VALID_PAYMENT_MODE)),
    Check("orders.discount_pct_0_100", "Validity", CRITICAL, lambda df, c: num(df["discount_pct"]).notna() & ~num(df["discount_pct"]).between(0, 100)),
    Check("orders.channel_in_list", "Validity", WARNING, lambda df, c: not_in(df["channel"], C.VALID_CHANNEL)),
    # Accuracy
    Check("orders.quantity_positive", "Accuracy", CRITICAL, lambda df, c: num(df["quantity"]).isna() | (num(df["quantity"]) <= 0)),
    Check("orders.unit_price_positive", "Accuracy", CRITICAL, lambda df, c: num(df["unit_price"]).isna() | (num(df["unit_price"]) <= 0)),
    Check("orders.order_amount_matches_formula", "Accuracy", CRITICAL, _amount_mismatch),
    Check("orders.delivery_minutes_below_120", "Accuracy", CRITICAL, lambda df, c: num(df["delivery_minutes"]) >= C.DELIVERY_MAX_HARD),
    Check("orders.delivery_minutes_in_normal_range", "Accuracy", WARNING,
          lambda df, c: num(df["delivery_minutes"]).notna() & (num(df["delivery_minutes"]) < C.DELIVERY_MAX_HARD)
          & ~num(df["delivery_minutes"]).between(C.DELIVERY_MIN_NORMAL, C.DELIVERY_MAX_NORMAL)),
    # Consistency
    Check("orders.customer_id_in_master", "Consistency", CRITICAL, lambda df, c: not_in_master(df["customer_id"], "customer_ids", c)),
    Check("orders.product_id_in_master", "Consistency", CRITICAL, lambda df, c: not_in_master(df["product_id"], "product_ids", c)),
    Check("orders.store_id_in_master", "Consistency", CRITICAL, lambda df, c: not_in_master(df["store_id"], "store_ids", c)),
    Check("orders.customer_row_quarantined", "Consistency", CRITICAL, lambda df, c: master_quarantined(df["customer_id"], "customer_ids", c),
          description="the customer exists but its master row failed a Critical rule"),
    Check("orders.product_row_quarantined", "Consistency", CRITICAL, lambda df, c: master_quarantined(df["product_id"], "product_ids", c)),
    Check("orders.store_row_quarantined", "Consistency", CRITICAL, lambda df, c: master_quarantined(df["store_id"], "store_ids", c)),
    Check("orders.unit_price_not_above_mrp", "Consistency", WARNING,
          lambda df, c: num(df["unit_price"]) > num(df["product_id"].map(c.get("product_mrp", {})))),
    # Timeliness
    Check("orders.order_ts_not_future", "Timeliness", CRITICAL, lambda df, c: future(df["order_ts"], c)),
    Check("orders.latest_order_not_stale", "Timeliness", WARNING, stale_dataset("order_ts"), row_level=False),
    Check("orders.volume_not_anomalous", "Timeliness", WARNING, low_volume(C.EXPECTED_MIN_ORDER_ROWS), row_level=False,
          description=f"fewer than {C.EXPECTED_MIN_ORDER_ROWS} order rows received"),
]

# ─────────────────────────── PAYMENTS ───────────────────────────
PAYMENT_CHECKS = [
    Check("payments.payment_id_not_null", "Completeness", CRITICAL, lambda df, c: is_blank(df["payment_id"])),
    Check("payments.order_id_not_null", "Completeness", CRITICAL, lambda df, c: is_blank(df["order_id"])),
    Check("payments.amount_paid_not_null", "Completeness", CRITICAL, lambda df, c: num(df["amount_paid"]).isna()),
    Check("payments.gateway_ref_present_for_non_cod", "Completeness", WARNING,
          lambda df, c: is_blank(df["gateway_ref"]) & (df["payment_mode"] != "Cash on Delivery")),
    Check("payments.payment_id_unique", "Uniqueness", CRITICAL, lambda df, c: df["payment_id"].duplicated(keep="first")),
    Check("payments.payment_ts_parses", "Validity", CRITICAL, lambda df, c: df["payment_ts"].notna() & ts(df["payment_ts"]).isna()),
    Check("payments.payment_status_in_list", "Validity", CRITICAL, lambda df, c: not_in(df["payment_status"], C.VALID_PAYMENT_STATUS)),
    Check("payments.payment_mode_in_list", "Validity", CRITICAL, lambda df, c: not_in(df["payment_mode"], C.VALID_PAYMENT_MODE)),
    Check("payments.success_amount_positive", "Accuracy", CRITICAL,
          lambda df, c: (df["payment_status"] == "SUCCESS") & (num(df["amount_paid"]) <= 0)),
    Check("payments.order_id_in_orders", "Consistency", CRITICAL, lambda df, c: not_in_master(df["order_id"], "order_ids_raw", c)),
    Check("payments.mode_matches_order", "Consistency", WARNING,
          lambda df, c: df["order_id"].map(c.get("order_mode", {})).notna()
          & (df["order_id"].map(c.get("order_mode", {})) != df["payment_mode"])),
    Check("payments.success_amount_equals_order_amount", "Consistency", WARNING,
          lambda df, c: (df["payment_status"] == "SUCCESS")
          & num(df["order_id"].map(c.get("order_amount", {}))).notna()
          & ((num(df["amount_paid"]) - num(df["order_id"].map(c.get("order_amount", {})))).abs() > 0.01)),
    Check("payments.payment_ts_not_future", "Timeliness", CRITICAL, lambda df, c: future(df["payment_ts"], c)),
    Check("payments.latest_payment_not_stale", "Timeliness", WARNING, stale_dataset("payment_ts"), row_level=False),
]

# ─────────────────────────── CUSTOMERS ───────────────────────────
CUSTOMER_CHECKS = [
    Check("customers.customer_id_not_null", "Completeness", CRITICAL, lambda df, c: is_blank(df["customer_id"])),
    Check("customers.full_name_not_null", "Completeness", WARNING, lambda df, c: is_blank(df["full_name"])),
    Check("customers.city_not_null", "Completeness", WARNING, lambda df, c: is_blank(df["city"])),
    Check("customers.email_not_null", "Completeness", WARNING, lambda df, c: is_blank(df["email"])),
    # keep="last": the export is chronological, the newest row is the current address/phone
    Check("customers.customer_id_unique", "Uniqueness", CRITICAL, lambda df, c: df["customer_id"].duplicated(keep="last"),
          description="older duplicate rows are superseded by the last row"),
    Check("customers.email_format", "Validity", WARNING, lambda df, c: df["email"].notna() & ~df["email"].str.match(EMAIL_RE, na=False)),
    Check("customers.phone_10_digits", "Validity", WARNING, lambda df, c: df["phone"].notna() & ~df["phone"].str.match(PHONE_RE, na=False)),
    Check("customers.pincode_6_digits", "Validity", WARNING, lambda df, c: df["pincode"].notna() & ~df["pincode"].str.match(PINCODE_RE, na=False)),
    Check("customers.is_active_Y_N", "Validity", WARNING, lambda df, c: not_in(df["is_active"], ["Y", "N"])),
    Check("customers.signup_date_parses", "Validity", CRITICAL, lambda df, c: df["signup_date"].notna() & ts(df["signup_date"], DATE_FORMAT).isna()),
    Check("customers.loyalty_tier_in_list", "Consistency", CRITICAL, lambda df, c: not_in(df["loyalty_tier"], C.VALID_LOYALTY_TIER)),
    Check("customers.loyalty_tier_casing_was_fixed", "Consistency", WARNING,
          lambda df, c: c["raw"]["loyalty_tier"].notna() & (c["raw"]["loyalty_tier"].str.strip() != df["loyalty_tier"])),
    Check("customers.signup_date_not_future", "Timeliness", WARNING, lambda df, c: future(df["signup_date"], c, DATE_FORMAT),
          description="master attribute is wrong but the customer (and their orders) are real → keep, flag"),
    Check("customers.signup_not_after_first_order", "Accuracy", WARNING,
          lambda df, c: ts(df["signup_date"], DATE_FORMAT) > df["customer_id"].map(c.get("first_order_ts", {}))),
]

# ─────────────────────────── PRODUCTS ───────────────────────────
PRODUCT_CHECKS = [
    Check("products.product_id_not_null", "Completeness", CRITICAL, lambda df, c: is_blank(df["product_id"])),
    Check("products.category_not_null", "Completeness", WARNING, lambda df, c: is_blank(df["category"]),
          description="blank category is loaded as 'Unknown' so the product's revenue stays visible"),
    Check("products.product_name_not_null", "Completeness", CRITICAL, lambda df, c: is_blank(df["product_name"])),
    Check("products.superseded_old_row", "Uniqueness", CRITICAL,
          lambda df, c: df["product_id"].duplicated(keep=False) & df["product_name"].str.endswith("(old)", na=False)),
    Check("products.product_id_unique", "Uniqueness", CRITICAL,
          lambda df, c: df.assign(_old=df["product_name"].str.endswith("(old)", na=False))
          .sort_values("_old")["product_id"].duplicated(keep="first").reindex(df.index)),
    Check("products.is_perishable_bool", "Validity", WARNING, lambda df, c: not_in(df["is_perishable"], ["True", "False"])),
    Check("products.launch_date_parses", "Validity", WARNING, lambda df, c: df["launch_date"].notna() & ts(df["launch_date"], DATE_FORMAT).isna()),
    Check("products.mrp_positive", "Accuracy", WARNING, lambda df, c: num(df["mrp"]).isna() | (num(df["mrp"]) <= 0),
          description="mrp is a catalogue attribute, not used in revenue/margin → flag, do not drop"),
    Check("products.cost_price_positive", "Accuracy", CRITICAL, lambda df, c: num(df["cost_price"]).isna() | (num(df["cost_price"]) <= 0)),
    Check("products.cost_price_not_above_mrp", "Accuracy", WARNING, lambda df, c: num(df["cost_price"]) > num(df["mrp"])),
    Check("products.category_in_known_list", "Consistency", WARNING, lambda df, c: not_in(df["category"], C.VALID_CATEGORY)),
    Check("products.launch_date_not_future", "Timeliness", CRITICAL, lambda df, c: future(df["launch_date"], c, DATE_FORMAT)),
]

# ─────────────────────────── STORES ───────────────────────────
STORE_CHECKS = [
    Check("stores.store_id_not_null", "Completeness", CRITICAL, lambda df, c: is_blank(df["store_id"])),
    Check("stores.zone_not_null", "Completeness", WARNING, lambda df, c: is_blank(df["zone"]),
          description="blank zone is filled with 'Unknown' so the store's revenue is not lost"),
    Check("stores.store_id_unique", "Uniqueness", CRITICAL, lambda df, c: df["store_id"].duplicated(keep="first")),
    Check("stores.pincode_6_digits", "Validity", WARNING, lambda df, c: df["pincode"].notna() & ~df["pincode"].str.match(PINCODE_RE, na=False)),
    Check("stores.capacity_positive", "Accuracy", WARNING, lambda df, c: num(df["capacity_orders_per_day"]).isna() | (num(df["capacity_orders_per_day"]) <= 0)),
    Check("stores.zone_in_list", "Consistency", WARNING, lambda df, c: not_in(df["zone"], C.VALID_ZONE)),
    Check("stores.open_date_not_future", "Timeliness", CRITICAL, lambda df, c: future(df["open_date"], c, DATE_FORMAT)),
]

CHECKS = {
    "orders": ORDER_CHECKS,
    "payments": PAYMENT_CHECKS,
    "customers": CUSTOMER_CHECKS,
    "products": PRODUCT_CHECKS,
    "stores": STORE_CHECKS,
}


# ─────────────────────────── runner ───────────────────────────
def run_checks(df: pd.DataFrame, checks: list[Check], ctx: dict):
    """Returns (passed_df, failed_df, [CheckResult]).

    failed_df has an extra `failure_reason` column (';'-joined Critical rule names).
    passed_df has an extra `dq_warnings` column (';'-joined Warning rule names or NULL).
    """
    n = len(df)
    critical_reasons = pd.Series([[] for _ in range(n)], index=df.index, dtype=object)
    warning_reasons = pd.Series([[] for _ in range(n)], index=df.index, dtype=object)
    results: list[CheckResult] = []

    for chk in checks:
        if chk.row_level:
            mask = chk.fn(df, ctx)
            mask = pd.Series(mask, index=df.index).fillna(False).astype(bool)
            failed = int(mask.sum())
            target = critical_reasons if chk.severity == CRITICAL else warning_reasons
            for idx in df.index[mask]:
                target[idx].append(chk.name)
        else:
            failed = int(chk.fn(df, ctx))
        status = "PASS" if failed == 0 else ("FAIL" if chk.severity == CRITICAL else "WARN")
        results.append(CheckResult(chk.name, chk.dimension, chk.severity, n, failed, status))

    is_failed = critical_reasons.map(len) > 0
    failed_df = df[is_failed].copy()
    failed_df["failure_reason"] = critical_reasons[is_failed].map(lambda r: ";".join(r))
    failed_df["dq_warnings"] = warning_reasons[is_failed].map(lambda r: ";".join(r) or None)
    passed_df = df[~is_failed].copy()
    passed_df["dq_warnings"] = warning_reasons[~is_failed].map(lambda r: ";".join(r) or None)
    return passed_df, failed_df, results


def summarise(results: list[CheckResult]) -> str:
    rows = [f"  {r.status:4} {r.severity:8} {r.dimension:12} {r.check_name:48} failed {r.rows_failed:>5} / {r.rows_checked}"
            for r in results]
    return "\n".join(rows)
