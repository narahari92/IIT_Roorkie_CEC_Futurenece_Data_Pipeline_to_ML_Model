"""
Part 0 — Discovery & data profiling for NammaMart.

Run from the project root:
    .venv/bin/python include/nammamart/profiling/profile_data.py > include/nammamart/profiling/profile_report.md

Everything is read-only. Nothing in include/data/nammamart/ is modified.
Sections:
  1. Per-file profile: rows, nulls, duplicate keys, numeric/date min-max, category distinct values
  2. Referential integrity (4 relationships)
  3. Business-rule checks
  4. Machine-generated issue candidates (feed for the Data Issues Log)
"""
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
DATA = ROOT / "include" / "data" / "nammamart"

FILES = {
    "orders":    ("orders.csv", "order_id"),
    "payments":  ("payments.csv", "payment_id"),
    "customers": ("customers.csv", "customer_id"),
    "products":  ("products.csv", "product_id"),
    "stores":    ("stores.csv", "store_id"),
    "increment": ("orders_2026-10-01_increment.csv", "order_id"),
}
CATEGORY_COLS = {
    "orders":    ["order_status", "payment_mode", "channel"],
    "increment": ["order_status", "payment_mode", "channel"],
    "payments":  ["payment_mode", "payment_status"],
    "customers": ["loyalty_tier", "is_active", "city"],
    "products":  ["category", "unit", "is_perishable"],
    "stores":    ["zone"],
}
DATE_COLS = {
    "orders": ["order_ts"], "increment": ["order_ts"], "payments": ["payment_ts"],
    "customers": ["signup_date"], "products": ["launch_date"], "stores": ["open_date"],
}
VALID_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_PAYMODE = {"UPI", "Card", "Cash on Delivery", "Wallet"}
RUN_DATE = pd.Timestamp("2026-10-20")  # today, for timeliness checks

issues = []  # (file, row_key, column, problem, dimension, rows_affected, example)


def log(file, key, col, problem, dim, n, example=""):
    issues.append(dict(file=file, row_key=key, column=col, problem=problem,
                       dimension=dim, rows=n, example=str(example)[:80]))


def h(title, level=2):
    print(f"\n{'#' * level} {title}\n")


def read_all():
    # dtype=str keeps everything as text so we see the raw export exactly as received
    return {k: pd.read_csv(DATA / fn, dtype=str, keep_default_na=False) for k, (fn, _) in FILES.items()}


def blank_to_na(df):
    return df.replace({"": pd.NA})


# ───────────────────────── 1. per-file profile ─────────────────────────
def profile(name, raw):
    fn, key = FILES[name]
    df = blank_to_na(raw)
    h(f"{fn}  ({len(df)} rows, {df.shape[1]} cols)")

    # nulls / blanks
    nulls = df.isna().sum()
    nulls = nulls[nulls > 0]
    print("**Nulls / blanks per column:**", "none" if nulls.empty else "")
    for c, n in nulls.items():
        print(f"- `{c}`: {n}")
        ex_key = df.loc[df[c].isna(), key].head(3).tolist()
        log(fn, ",".join(map(str, ex_key)), c, f"{n} null/blank values", "Completeness", n)

    # whitespace / casing anomalies in text columns
    for c in df.columns:
        s = df[c].dropna()
        ws = s[s != s.str.strip()]
        if len(ws):
            print(f"- `{c}`: {len(ws)} values with leading/trailing whitespace e.g. {ws.head(2).tolist()}")
            log(fn, ",".join(df.loc[ws.index, key].head(3)), c, f"{len(ws)} values with leading/trailing whitespace", "Validity", len(ws), ws.iloc[0])

    # duplicate keys
    dup_full = df.duplicated(keep=False).sum()
    dup_key = df[key].duplicated(keep=False).sum()
    dup_key_vals = df.loc[df[key].duplicated(), key].unique().tolist()
    print(f"\n**Duplicate primary key `{key}`:** {dup_key} rows involved ({len(dup_key_vals)} keys repeated); exact-duplicate rows: {dup_full}")
    if dup_key_vals:
        print(f"- repeated keys e.g. {dup_key_vals[:8]}")
        log(fn, ",".join(dup_key_vals[:5]), key, f"{len(dup_key_vals)} keys appear more than once ({dup_full} exact dupes, {dup_key-dup_full} differing)", "Uniqueness", dup_key)

    # numeric columns
    print("\n**Numeric columns (min / max / non-numeric count):**")
    for c in df.columns:
        if c in DATE_COLS.get(name, []) or c == key:
            continue
        num = pd.to_numeric(df[c], errors="coerce")
        parsed = num.notna().sum()
        nonblank = df[c].notna().sum()
        if parsed >= 0.8 * nonblank and parsed > 0:
            bad = nonblank - parsed
            print(f"- `{c}`: min={num.min()}  max={num.max()}  mean={num.mean():.2f}  non-numeric={bad}")
            if bad:
                ex = df.loc[num.isna() & df[c].notna(), [key, c]].head(3)
                log(fn, ",".join(ex[key]), c, f"{bad} non-numeric values", "Validity", bad, ex[c].tolist())

    # date columns
    print("\n**Date / timestamp columns:**")
    for c in DATE_COLS.get(name, []):
        ts = pd.to_datetime(df[c], errors="coerce", format="mixed")
        bad = df[c].notna().sum() - ts.notna().sum()
        print(f"- `{c}`: min={ts.min()}  max={ts.max()}  unparseable={bad}")
        if bad:
            ex = df.loc[ts.isna() & df[c].notna(), [key, c]].head(3)
            log(fn, ",".join(ex[key]), c, f"{bad} unparseable timestamps", "Validity", bad, ex[c].tolist())
        fut = (ts > RUN_DATE).sum()
        if fut:
            ex = df.loc[ts > RUN_DATE, [key, c]].head(3)
            log(fn, ",".join(ex[key]), c, f"{fut} future-dated values (> {RUN_DATE.date()})", "Timeliness", fut, ex[c].tolist())
            print(f"  - future-dated: {fut} e.g. {ex[c].tolist()}")
        # format consistency
        fmts = df[c].dropna().str.replace(r"\d", "9", regex=True).value_counts()
        if len(fmts) > 1:
            print(f"  - mixed formats: {fmts.to_dict()}")
            minority = fmts.index[1:]
            n = df[c].str.replace(r"\d", "9", regex=True).isin(minority).sum()
            ex = df.loc[df[c].str.replace(r"\d", "9", regex=True).isin(minority), [key, c]].head(3)
            log(fn, ",".join(ex[key]), c, f"{n} values in non-standard date format {list(minority)}", "Validity", n, ex[c].tolist())

    # categories
    print("\n**Category columns (distinct values):**")
    for c in CATEGORY_COLS.get(name, []):
        vc = df[c].value_counts(dropna=False)
        print(f"- `{c}`: {vc.to_dict()}")
    return df


# ───────────────────────── 2. referential integrity ─────────────────────────
def ref_integrity(d):
    h("Referential integrity")
    o, p, c, pr, s, inc = (d[k] for k in ["orders", "payments", "customers", "products", "stores", "increment"])
    checks = [
        ("orders.csv", o, "customer_id", c, "customer_id"),
        ("orders.csv", o, "product_id", pr, "product_id"),
        ("orders.csv", o, "store_id", s, "store_id"),
        ("payments.csv", p, "order_id", o, "order_id"),
        ("orders_2026-10-01_increment.csv", inc, "customer_id", c, "customer_id"),
        ("orders_2026-10-01_increment.csv", inc, "product_id", pr, "product_id"),
        ("orders_2026-10-01_increment.csv", inc, "store_id", s, "store_id"),
    ]
    for fn, child, ccol, parent, pcol in checks:
        key = FILES[[k for k, v in FILES.items() if v[0] == fn][0]][1]
        parent_keys = set(parent[pcol].dropna().str.strip())
        orphan_mask = child[ccol].notna() & ~child[ccol].str.strip().isin(parent_keys)
        n = orphan_mask.sum()
        ex = child.loc[orphan_mask, [key, ccol]].head(5)
        print(f"- `{fn}.{ccol}` → `{pcol}`: {n} orphan rows" + (f"; e.g. {ex[ccol].unique()[:5].tolist()}" if n else ""))
        if n:
            log(fn, ",".join(ex[key]), ccol, f"{n} rows reference a {ccol} that does not exist in the master", "Consistency", n, ex[ccol].unique()[:3].tolist())
    # orders without payments (cancelled are expected)
    paid = set(p["order_id"])
    o_status = o["order_status"].str.strip().str.upper()
    no_pay = o[~o["order_id"].isin(paid)]
    print(f"- orders without any payment: {len(no_pay)}; by status: {no_pay['order_status'].str.strip().str.upper().value_counts().to_dict()}")
    nd = no_pay[no_pay["order_status"].str.strip().str.upper() == "DELIVERED"]
    if len(nd):
        log("orders.csv / payments.csv", ",".join(nd["order_id"].head(5)), "order_id", f"{len(nd)} DELIVERED orders have no payment row", "Consistency", len(nd))
    # cancelled orders that DO have payments
    cp = o[(o_status == "CANCELLED") & o["order_id"].isin(paid)]
    print(f"- CANCELLED orders that have a payment row: {len(cp)}")
    if len(cp):
        pstat = p[p["order_id"].isin(cp["order_id"])]["payment_status"].value_counts().to_dict()
        print(f"  - their payment statuses: {pstat}")
    # payments per order
    ppo = p.groupby("order_id").size()
    multi = ppo[ppo > 1]
    print(f"- orders with >1 payment row: {len(multi)} e.g. {multi.head(5).to_dict()}")
    if len(multi):
        log("payments.csv", ",".join(multi.index[:5]), "order_id", f"{len(multi)} orders have more than one payment row", "Uniqueness", int(multi.sum()))


# ───────────────────────── 3. business rules ─────────────────────────
def business_rules(d):
    h("Business-rule checks")
    o, p, c, pr, s, inc = (d[k] for k in ["orders", "payments", "customers", "products", "stores", "increment"])

    for fn, df in [("orders.csv", o), ("orders_2026-10-01_increment.csv", inc)]:
        print(f"\n### {fn}")
        q = pd.to_numeric(df["quantity"], errors="coerce")
        up = pd.to_numeric(df["unit_price"], errors="coerce")
        disc = pd.to_numeric(df["discount_pct"], errors="coerce")
        amt = pd.to_numeric(df["order_amount"], errors="coerce")
        dm = pd.to_numeric(df["delivery_minutes"], errors="coerce")
        status = df["order_status"].str.strip().str.upper()
        pm = df["payment_mode"]

        expected = (q * up * (1 - disc / 100)).round(2)
        mism = (expected - amt).abs() > 0.01
        mism &= expected.notna() & amt.notna()
        print(f"- order_amount ≠ qty×price×(1−disc/100): {mism.sum()} rows")
        if mism.sum():
            ex = df.loc[mism, ["order_id", "quantity", "unit_price", "discount_pct", "order_amount"]].head(5)
            ex["expected"] = expected[mism].head(5).values
            print(ex.to_string(index=False))
            log(fn, ",".join(ex["order_id"]), "order_amount", f"{mism.sum()} rows where order_amount does not match the formula", "Accuracy", mism.sum(), f"{ex.iloc[0]['order_amount']} vs expected {ex.iloc[0]['expected']}")

        bad_status = ~df["order_status"].isin(VALID_STATUS)
        print(f"- order_status not in {sorted(VALID_STATUS)} (exact match): {bad_status.sum()} → {df.loc[bad_status,'order_status'].value_counts().to_dict()}")
        if bad_status.sum():
            log(fn, ",".join(df.loc[bad_status, "order_id"].head(5)), "order_status", f"{bad_status.sum()} rows with status outside allowed list (casing/whitespace/unknown): {df.loc[bad_status,'order_status'].unique()[:5].tolist()}", "Validity", bad_status.sum())

        bad_pm = ~pm.isin(VALID_PAYMODE)
        print(f"- payment_mode not in allowed list (exact): {bad_pm.sum()} → {pm[bad_pm].value_counts().to_dict()}")
        if bad_pm.sum():
            log(fn, ",".join(df.loc[bad_pm, "order_id"].head(5)), "payment_mode", f"{bad_pm.sum()} rows with payment_mode outside allowed list: {pm[bad_pm].unique()[:6].tolist()}", "Validity", bad_pm.sum())

        print(f"- quantity <= 0: {(q <= 0).sum()}   quantity null/non-numeric: {q.isna().sum()}")
        if (q <= 0).sum():
            ex = df.loc[q <= 0, ["order_id", "quantity", "order_status"]].head(5)
            print(ex.to_string(index=False))
            log(fn, ",".join(ex["order_id"]), "quantity", f"{(q<=0).sum()} rows with quantity <= 0", "Accuracy", (q <= 0).sum(), ex["quantity"].tolist())
        print(f"- unit_price <= 0: {(up <= 0).sum()}")
        if (up <= 0).sum():
            log(fn, ",".join(df.loc[up <= 0, "order_id"].head(5)), "unit_price", f"{(up<=0).sum()} rows with unit_price <= 0", "Accuracy", (up <= 0).sum())
        print(f"- order_amount < 0: {(amt < 0).sum()}")
        if (amt < 0).sum():
            log(fn, ",".join(df.loc[amt < 0, "order_id"].head(5)), "order_amount", f"{(amt<0).sum()} rows with negative order_amount", "Accuracy", (amt < 0).sum())
        bad_disc = (disc < 0) | (disc > 100)
        print(f"- discount_pct outside 0–100: {bad_disc.sum()}; distinct discounts: {sorted(disc.dropna().unique().tolist())}")
        if bad_disc.sum():
            log(fn, ",".join(df.loc[bad_disc, "order_id"].head(5)), "discount_pct", f"{bad_disc.sum()} rows with discount outside 0–100", "Validity", bad_disc.sum())

        deliv_no_min = (status == "DELIVERED") & dm.isna()
        print(f"- DELIVERED with no delivery_minutes: {deliv_no_min.sum()}")
        if deliv_no_min.sum():
            log(fn, ",".join(df.loc[deliv_no_min, "order_id"].head(5)), "delivery_minutes", f"{deliv_no_min.sum()} DELIVERED orders missing delivery_minutes", "Completeness", deliv_no_min.sum())
        out_range = dm.notna() & ((dm < 8) | (dm > 35))
        print(f"- delivery_minutes outside normal 8–35: {out_range.sum()}; >120: {(dm > 120).sum()}; <=0: {(dm <= 0).sum()}")
        print(f"  distribution of out-of-range values: {sorted(dm[out_range].unique().tolist())[:30]}")
        if (dm > 120).sum():
            ex = df.loc[dm > 120, ["order_id", "delivery_minutes"]].head(5)
            log(fn, ",".join(ex["order_id"]), "delivery_minutes", f"{(dm>120).sum()} rows with delivery_minutes > 120 (impossible for 10-min promise)", "Accuracy", (dm > 120).sum(), ex["delivery_minutes"].tolist())
        if (dm <= 0).sum():
            log(fn, ",".join(df.loc[dm <= 0, "order_id"].head(5)), "delivery_minutes", f"{(dm<=0).sum()} rows with delivery_minutes <= 0", "Accuracy", (dm <= 0).sum())
        nd_with_min = (status != "DELIVERED") & dm.notna()
        print(f"- non-DELIVERED rows that still carry delivery_minutes: {nd_with_min.sum()} (by status: {status[nd_with_min].value_counts().to_dict()})")

        # test entries
        test_mask = df.apply(lambda r: r.astype(str).str.contains(r"\btest\b|dummy|sample", case=False, regex=True).any(), axis=1)
        print(f"- rows containing 'test'/'dummy': {test_mask.sum()}")
        if test_mask.sum():
            print(df[test_mask].head(5).to_string(index=False))
            log(fn, ",".join(df.loc[test_mask, "order_id"].head(5)), "*", f"{test_mask.sum()} rows look like test entries", "Validity", test_mask.sum())
        # orders outside september
        ts = pd.to_datetime(df["order_ts"], errors="coerce", format="mixed")
        if fn == "orders.csv":
            outside = ts.notna() & ((ts < "2026-09-01") | (ts >= "2026-10-01"))
            print(f"- order_ts outside Sept 2026: {outside.sum()} e.g. {df.loc[outside,'order_ts'].head(5).tolist()}")
        # price vs product mrp
        prm = pr.drop_duplicates("product_id").set_index("product_id")
        mrp = pd.to_numeric(df["product_id"].map(prm["mrp"]), errors="coerce")
        over = up > mrp
        print(f"- unit_price > product mrp: {over.sum()}")
        if over.sum():
            ex = df.loc[over, ["order_id", "product_id", "unit_price"]].head(5)
            ex["mrp"] = mrp[over].head(5).values
            print(ex.to_string(index=False))

    print("\n### payments.csv")
    ap = pd.to_numeric(p["amount_paid"], errors="coerce")
    print(f"- payment_status values: {p['payment_status'].value_counts().to_dict()}")
    print(f"- payment_mode values: {p['payment_mode'].value_counts().to_dict()}")
    bad_pm = ~p["payment_mode"].isin(VALID_PAYMODE)
    if bad_pm.sum():
        log("payments.csv", ",".join(p.loc[bad_pm, "payment_id"].head(5)), "payment_mode", f"{bad_pm.sum()} rows with payment_mode outside allowed list: {p.loc[bad_pm,'payment_mode'].unique()[:6].tolist()}", "Validity", bad_pm.sum())
    print(f"- amount_paid <= 0: {(ap <= 0).sum()}; null: {ap.isna().sum()}")
    if (ap <= 0).sum():
        log("payments.csv", ",".join(p.loc[ap <= 0, "payment_id"].head(5)), "amount_paid", f"{(ap<=0).sum()} rows with amount_paid <= 0", "Accuracy", (ap <= 0).sum())
    gw_blank = p["gateway_ref"].eq("")
    print(f"- blank gateway_ref: {gw_blank.sum()}; by payment_mode: {p.loc[gw_blank,'payment_mode'].value_counts().to_dict()}")
    non_cod_blank = gw_blank & (p["payment_mode"].str.strip() != "Cash on Delivery")
    if non_cod_blank.sum():
        log("payments.csv", ",".join(p.loc[non_cod_blank, "payment_id"].head(5)), "gateway_ref", f"{non_cod_blank.sum()} non-COD payments have blank gateway_ref", "Completeness", non_cod_blank.sum())
    # SUCCESS payment must equal order amount
    o_amt = o.drop_duplicates("order_id").set_index("order_id")["order_amount"]
    o_amt = pd.to_numeric(o_amt, errors="coerce")
    joined = p.assign(order_amt=p["order_id"].map(o_amt), ap=ap)
    succ = joined[joined["payment_status"].str.strip().str.upper() == "SUCCESS"]
    mism = succ[(succ["order_amt"] - succ["ap"]).abs() > 0.01]
    print(f"- SUCCESS payments where amount_paid ≠ order_amount: {len(mism)} (of {len(succ)} SUCCESS)")
    if len(mism):
        print(mism[["payment_id", "order_id", "amount_paid", "order_amt"]].head(8).to_string(index=False))
        diff = (mism["ap"] - mism["order_amt"])
        print(f"  - diff summary: min={diff.min():.2f} max={diff.max():.2f}; sum of mismatch={diff.abs().sum():.2f}")
        log("payments.csv", ",".join(mism["payment_id"].head(5)), "amount_paid", f"{len(mism)} SUCCESS payments do not equal order_amount", "Consistency", len(mism), f"{mism.iloc[0]['amount_paid']} vs order {mism.iloc[0]['order_amt']}")
    # payment mode agreement with order
    o_pm = o.drop_duplicates("order_id").set_index("order_id")["payment_mode"].str.strip().str.upper()
    pm_mis = joined[joined["order_id"].map(o_pm).notna() & (joined["order_id"].map(o_pm) != joined["payment_mode"].str.strip().str.upper())]
    print(f"- payment_mode differs between order and payment: {len(pm_mis)}")
    if len(pm_mis):
        print(pm_mis[["payment_id", "order_id", "payment_mode"]].assign(order_mode=pm_mis["order_id"].map(o_pm)).head(5).to_string(index=False))
        log("payments.csv", ",".join(pm_mis["payment_id"].head(5)), "payment_mode", f"{len(pm_mis)} payments whose mode differs from the order's payment_mode", "Consistency", len(pm_mis))
    # payment before order
    pts = pd.to_datetime(p["payment_ts"], errors="coerce", format="mixed")
    ots = pd.to_datetime(o.drop_duplicates("order_id").set_index("order_id")["order_ts"], errors="coerce", format="mixed")
    before = pts < p["order_id"].map(ots)
    print(f"- payment_ts earlier than order_ts: {before.sum()}")
    if before.sum():
        log("payments.csv", ",".join(p.loc[before, "payment_id"].head(5)), "payment_ts", f"{before.sum()} payments timestamped before their order", "Consistency", before.sum())

    print("\n### customers.csv")
    email_ok = c["email"].str.match(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$", na=False)
    bad_email = c["email"].ne("") & ~email_ok
    print(f"- invalid email format: {bad_email.sum()} e.g. {c.loc[bad_email,'email'].head(5).tolist()}")
    if bad_email.sum():
        log("customers.csv", ",".join(c.loc[bad_email, "customer_id"].head(5)), "email", f"{bad_email.sum()} malformed emails", "Validity", bad_email.sum(), c.loc[bad_email, "email"].head(2).tolist())
    phone_ok = c["phone"].str.fullmatch(r"[6-9]\d{9}", na=False)
    bad_phone = c["phone"].ne("") & ~phone_ok
    print(f"- phone not 10-digit Indian mobile: {bad_phone.sum()} e.g. {c.loc[bad_phone,'phone'].head(5).tolist()}")
    if bad_phone.sum():
        log("customers.csv", ",".join(c.loc[bad_phone, "customer_id"].head(5)), "phone", f"{bad_phone.sum()} phones not 10 digits", "Validity", bad_phone.sum(), c.loc[bad_phone, "phone"].head(2).tolist())
    pin_ok = c["pincode"].str.fullmatch(r"5600\d\d|56\d{4}", na=False)
    bad_pin = c["pincode"].ne("") & ~pin_ok
    print(f"- pincode not 6-digit Bengaluru: {bad_pin.sum()} e.g. {c.loc[bad_pin,'pincode'].head(5).tolist()}")
    if bad_pin.sum():
        log("customers.csv", ",".join(c.loc[bad_pin, "customer_id"].head(5)), "pincode", f"{bad_pin.sum()} invalid pincodes", "Validity", bad_pin.sum(), c.loc[bad_pin, "pincode"].head(2).tolist())
    tiers = c["loyalty_tier"].value_counts().to_dict()
    canon = c["loyalty_tier"].str.strip().str.title().value_counts().to_dict()
    print(f"- loyalty_tier raw: {tiers}\n  → after trim+title-case: {canon}")
    n_noncanon = (c["loyalty_tier"] != c["loyalty_tier"].str.strip().str.title()).sum()
    if n_noncanon:
        log("customers.csv", ",".join(c.loc[c["loyalty_tier"] != c["loyalty_tier"].str.strip().str.title(), "customer_id"].head(5)), "loyalty_tier", f"{n_noncanon} rows with inconsistent casing/whitespace ({[k for k in tiers if k not in canon]})", "Consistency", n_noncanon)
    dup_email = c["email"].ne("") & c["email"].str.lower().str.strip().duplicated(keep=False)
    print(f"- duplicate emails (case-insens): {dup_email.sum()}")
    if dup_email.sum():
        log("customers.csv", ",".join(c.loc[dup_email, "customer_id"].head(5)), "email", f"{dup_email.sum()} rows share an email with another row", "Uniqueness", dup_email.sum())
    dup_phone = c["phone"].ne("") & c["phone"].duplicated(keep=False)
    print(f"- duplicate phones: {dup_phone.sum()}")
    if dup_phone.sum():
        log("customers.csv", ",".join(c.loc[dup_phone, "customer_id"].head(5)), "phone", f"{dup_phone.sum()} rows share a phone with another row", "Uniqueness", dup_phone.sum())
    # orders by inactive customers
    inactive = set(c.loc[c["is_active"].str.strip().str.upper().isin(["N", "FALSE", "0"]), "customer_id"])
    n_inact = o["customer_id"].isin(inactive).sum()
    print(f"- orders placed by is_active=N customers: {n_inact}")
    # signup after first order
    first_order = pd.to_datetime(o["order_ts"], errors="coerce", format="mixed").groupby(o["customer_id"]).min()
    signup = pd.to_datetime(c.drop_duplicates("customer_id").set_index("customer_id")["signup_date"], errors="coerce", format="mixed")
    cmp = pd.concat([first_order.rename("first_order"), signup.rename("signup")], axis=1).dropna()
    late = cmp[cmp["signup"] > cmp["first_order"]]
    print(f"- customers whose signup_date is after their first order: {len(late)}")
    if len(late):
        log("customers.csv", ",".join(late.index[:5]), "signup_date", f"{len(late)} customers signed up after their first order", "Consistency", len(late))

    print("\n### products.csv")
    mrp = pd.to_numeric(pr["mrp"], errors="coerce")
    cp = pd.to_numeric(pr["cost_price"], errors="coerce")
    print(f"- mrp <= 0: {(mrp <= 0).sum()}; cost_price <= 0: {(cp <= 0).sum()}; cost_price > mrp: {(cp > mrp).sum()}; mrp null: {mrp.isna().sum()}; cost null: {cp.isna().sum()}")
    if (mrp <= 0).sum():
        log("products.csv", ",".join(pr.loc[mrp <= 0, "product_id"].head(5)), "mrp", f"{(mrp<=0).sum()} products with mrp <= 0", "Accuracy", (mrp <= 0).sum(), pr.loc[mrp <= 0, "mrp"].tolist()[:3])
    if (cp <= 0).sum():
        log("products.csv", ",".join(pr.loc[cp <= 0, "product_id"].head(5)), "cost_price", f"{(cp<=0).sum()} products with cost_price <= 0", "Accuracy", (cp <= 0).sum())
    if (cp > mrp).sum():
        ex = pr.loc[cp > mrp, ["product_id", "mrp", "cost_price"]].head(5)
        print(ex.to_string(index=False))
        log("products.csv", ",".join(ex["product_id"]), "cost_price", f"{(cp>mrp).sum()} products where cost_price > mrp (negative margin)", "Accuracy", (cp > mrp).sum(), f"mrp {ex.iloc[0]['mrp']} cost {ex.iloc[0]['cost_price']}")
    cats = pr["category"].value_counts(dropna=False).to_dict()
    canon = pr["category"].str.strip().str.title().value_counts().to_dict()
    print(f"- category raw: {cats}")
    if len(cats) != len(canon):
        print(f"  → after trim+title: {canon}")
    # products never ordered / ordered products not in catalogue
    never = set(pr["product_id"]) - set(o["product_id"])
    print(f"- products never ordered in Sept: {len(never)}")
    # dup product rows
    dk = pr[pr["product_id"].duplicated(keep=False)].sort_values("product_id")
    if len(dk):
        print(f"- duplicate product_id rows:\n{dk.to_string(index=False)}")

    print("\n### stores.csv")
    cap = pd.to_numeric(s["capacity_orders_per_day"], errors="coerce")
    print(f"- capacity min/max: {cap.min()}/{cap.max()}; zone values: {s['zone'].value_counts().to_dict()}")
    zone_canon = s["zone"].str.strip().str.title()
    n_zone = (s["zone"] != zone_canon).sum()
    if n_zone:
        log("stores.csv", ",".join(s.loc[s["zone"] != zone_canon, "store_id"].head(5)), "zone", f"{n_zone} rows with inconsistent zone casing/whitespace: {s.loc[s['zone']!=zone_canon,'zone'].tolist()}", "Consistency", n_zone)
    pin_ok = s["pincode"].str.fullmatch(r"\d{6}", na=False)
    if (~pin_ok).sum():
        log("stores.csv", ",".join(s.loc[~pin_ok, "store_id"]), "pincode", f"{(~pin_ok).sum()} invalid pincodes", "Validity", (~pin_ok).sum(), s.loc[~pin_ok, "pincode"].tolist())
    # daily orders vs capacity
    o_day = pd.to_datetime(o["order_ts"], errors="coerce", format="mixed").dt.date
    per_day = o.groupby([o["store_id"].str.strip(), o_day]).size().rename("orders")
    capmap = pd.to_numeric(s.set_index("store_id")["capacity_orders_per_day"], errors="coerce")
    over = per_day[per_day > per_day.index.get_level_values(0).map(capmap)]
    print(f"- store-days exceeding capacity: {len(over)}")
    print(f"- stores referenced in orders: {sorted(o['store_id'].unique().tolist())}")
    print(s.to_string(index=False))

    print("\n### increment vs orders overlap")
    overlap = inc[inc["order_id"].isin(o["order_id"])]
    new = inc[~inc["order_id"].isin(o["order_id"])]
    print(f"- increment rows whose order_id already exists in orders.csv: {len(overlap)}; brand-new order_ids: {len(new)}")
    if len(overlap):
        merged = overlap.merge(o.drop_duplicates("order_id"), on="order_id", suffixes=("_new", "_old"))
        changed_cols = [c for c in o.columns if c != "order_id" and (merged[f"{c}_new"] != merged[f"{c}_old"]).any()]
        print(f"- columns that changed in the overlapping rows: {changed_cols}")
        print(merged[["order_id", "order_status_old", "order_status_new"]].to_string(index=False))
    its = pd.to_datetime(new["order_ts"], errors="coerce", format="mixed")
    print(f"- new rows order_ts range: {its.min()} → {its.max()}")


def main():
    print("# NammaMart — Part 0 profiling report\n")
    print(f"Generated by `include/nammamart/profiling/profile_data.py`; run date assumed {RUN_DATE.date()}.")
    d = read_all()
    h("1. Per-file profiles", 2)
    dfs = {name: profile(name, raw) for name, raw in d.items()}
    h("2. Referential integrity", 2)
    ref_integrity(d)
    h("3. Business rules", 2)
    business_rules(d)
    h("4. Auto-detected issue candidates", 2)
    out = pd.DataFrame(issues)
    print(out.to_markdown(index=False) if hasattr(out, "to_markdown") else out.to_string(index=False))
    out.to_csv(ROOT / "include" / "nammamart" / "profiling" / "issue_candidates.csv", index=False)


if __name__ == "__main__":
    main()
