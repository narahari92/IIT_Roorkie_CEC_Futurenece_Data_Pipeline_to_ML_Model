#!/usr/bin/env python
"""Build the Part F experiment fixtures as COPIES under include/data/nammamart/experiments/.
The original exports are never modified."""
import pandas as pd
from pathlib import Path
SRC = Path("include/data/nammamart/orders.csv"); OUT = Path("include/data/nammamart/experiments"); OUT.mkdir(exist_ok=True)
o = pd.read_csv(SRC, dtype=str, keep_default_na=False)

# 5 — new bad data: null customer, negative quantity, unknown store, SHIPPED, 2027 timestamp
bad = pd.DataFrame([
    ["ORD990001", "2026-09-15 10:00:00", "",       "S01", "P0001", "1",  "107.0", "0", "107.0",  "UPI",  "DELIVERED", "App", "12"],
    ["ORD990002", "2026-09-15 10:05:00", "C00199", "S01", "P0001", "-2", "107.0", "0", "-214.0", "UPI",  "DELIVERED", "App", "12"],
    ["ORD990003", "2026-09-15 10:10:00", "C00199", "S77", "P0001", "1",  "107.0", "0", "107.0",  "Card", "DELIVERED", "App", "12"],
    ["ORD990004", "2026-09-15 10:15:00", "C00199", "S01", "P0001", "1",  "107.0", "0", "107.0",  "UPI",  "SHIPPED",   "App", "12"],
    ["ORD990005", "2027-02-01 10:20:00", "C00199", "S01", "P0001", "1",  "107.0", "0", "107.0",  "UPI",  "DELIVERED", "App", "12"],
], columns=o.columns)
pd.concat([o, bad]).to_csv(OUT / "exp5_orders_bad.csv", index=False)

# 6 — schema change: order_amount renamed to total
o.rename(columns={"order_amount": "total"}).to_csv(OUT / "exp6_orders_schema.csv", index=False)

# 7 — volume anomaly: only 50 orders
o.head(50).to_csv(OUT / "exp7_orders_50.csv", index=False)

# 8 — distribution drift: unit_price ×100 for store S05 (order_amount scaled consistently so the formula still holds)
d = o.copy(); m = d["store_id"] == "S05"
d.loc[m, "unit_price"] = (pd.to_numeric(d.loc[m, "unit_price"]) * 100).map(lambda x: f"{x:.1f}")
d.loc[m, "order_amount"] = (pd.to_numeric(d.loc[m, "order_amount"]) * 100).round(2).astype(str)
d.to_csv(OUT / "exp8_orders_drift.csv", index=False)

# 9 — freshness: latest order_ts is 30 days before the run date (2026-10-21 → keep ≤ 2026-09-21)
ts = pd.to_datetime(o["order_ts"], errors="coerce", format="%Y-%m-%d %H:%M:%S")
o[ts <= "2026-09-21 23:59:59"].to_csv(OUT / "exp9_orders_stale.csv", index=False)

for f in sorted(OUT.glob("*.csv")):
    print(f"{f.name:28} {sum(1 for _ in open(f)) - 1:>5} rows")
