"""Value standardisation applied BEFORE quality checks (so casing/whitespace
problems are fixed and logged as Warnings instead of quarantining good rows)."""
import re
import pandas as pd

from .config import PAYMENT_MODE_MAP


def strip_all(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if out[c].dtype == object:
            out[c] = out[c].astype("string").str.strip()
    return out.replace({"": pd.NA, "N/A": pd.NA, "NA": pd.NA, "null": pd.NA, "None": pd.NA})


def canonical_payment_mode(v):
    if pd.isna(v):
        return v
    key = re.sub(r"\s+", "", str(v)).lower()
    return PAYMENT_MODE_MAP.get(key, str(v).strip())


def standardise_orders(df):
    out = strip_all(df)
    out["order_status"] = out["order_status"].str.upper()
    out["payment_mode"] = out["payment_mode"].map(canonical_payment_mode)
    out["channel"] = out["channel"].str.title()
    return out


def standardise_payments(df):
    out = strip_all(df)
    out["payment_status"] = out["payment_status"].str.upper()
    out["payment_mode"] = out["payment_mode"].map(canonical_payment_mode)
    return out


def standardise_customers(df):
    out = strip_all(df)
    out["loyalty_tier"] = out["loyalty_tier"].str.title()
    out["is_active"] = out["is_active"].str.upper()
    out["email"] = out["email"].str.lower()
    out["city"] = out["city"].str.title()
    return out


def standardise_products(df):
    out = strip_all(df)
    out["is_perishable"] = out["is_perishable"].str.lower().map({"true": "True", "false": "False", "1": "True", "0": "False"}).fillna(out["is_perishable"])
    return out


def standardise_stores(df):
    out = strip_all(df)
    out["zone"] = out["zone"].str.title()
    return out


STANDARDISERS = {
    "orders": standardise_orders,
    "payments": standardise_payments,
    "customers": standardise_customers,
    "products": standardise_products,
    "stores": standardise_stores,
}
