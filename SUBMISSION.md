# NammaMart ETL & ELT Pipeline — Submission

**Learner:** Hosur Narahari (`hosur_narahari`) · **Branch:** `hosur_narahari_assignment`
**Module:** FDAIE Module 2 · Data Engineering Foundations

| Section | Status |
|---|---|
| Part 0 — Discovery & Data Issues Log | ✅ below |
| Part A — ETL pipeline | ✅ below |
| Part B — ELT pipeline | ✅ below |
| Part C — Scheduling answers | ✅ below |
| Part D — Data quality dimensions | ✅ below |
| Part E — Governance answers | ✅ below |
| Part F — Observability results | ⏳ |
| Part G — Recommendation to CTO | ⏳ |
| Business questions (SQL + results) | ⏳ |

---

## Part 0 — Discovery and data profiling

**How it was done.** A read-only profiling script, [`include/nammamart/profiling/profile_data.py`](include/nammamart/profiling/profile_data.py), reads every CSV *as text* (so the export is seen exactly as received), then produces [`profile_report.md`](include/nammamart/profiling/profile_report.md) (full numbers) and [`issue_candidates.csv`](include/nammamart/profiling/issue_candidates.csv) (machine list with example row keys). Everything below is taken from that report. Nothing under `include/data/nammamart/` was edited. Run date assumed for timeliness checks: **2026-10-20**.

```bash
.venv/bin/python include/nammamart/profiling/profile_data.py > include/nammamart/profiling/profile_report.md
```

### 0.1 Profile summary per file

| File | Rows | Null / blank columns | Duplicate keys | Numeric ranges worth noting | Date range | Category values seen |
|---|---|---|---|---|---|---|
| `orders.csv` | 2,412 | `customer_id` 18, `delivery_minutes` 164 (156 CANCELLED + 8 DELIVERED), `order_ts` 1 | 12 `order_id`s appear twice (24 rows, exact duplicates) | `quantity` −3…5 · `discount_pct` 0…**250** · `order_amount` **−1593**…**9576** · `delivery_minutes` 8…**1440** | `order_ts` 2026-09-01 → **2027-01-01**; 5 unparseable (`N/A`, `2026-13-05`, `31/09/2026 10:00`) | `order_status`: DELIVERED 2182, CANCELLED 156, RETURNED 69, **`Pending??` 3, `delivered` 2** · `payment_mode`: UPI/Card/Cash on Delivery/Wallet + **`upi` 4, `UPI ` 2, `Cashh` 2, `CARD` 1, `cod` 1** · `channel`: App 2044, Web 368 |
| `payments.csv` | 2,228 | `gateway_ref` 301 (all COD — expected) | 4 `payment_id`s appear twice (8 rows, exact duplicates) | `amount_paid` **−1593**…9576; 10 rows ≤ 0 | `payment_ts` 2026-09-01 → **2027-01-01** (7 future) | `payment_status`: SUCCESS 2116, REFUNDED 68, FAILED 44 · `payment_mode` same 9 variants as orders |
| `customers.csv` | 405 | `city` 6 | 5 `customer_id`s appear twice (10 rows, **differing** phone/area → address change history) | `phone` has 7–8-digit values · `pincode` has `000000`, `5600XX` | `signup_date` 2024-06-01 → **2027-01-05** (3 future) | `loyalty_tier`: Bronze/Silver/Gold/Platinum + **`BRONZE` 3, `bronze` 3, `SILVER` 2** · `is_active`: Y 352, N 53 · `city`: Bengaluru 399, blank 6 |
| `products.csv` | 152 | `category` 3 | 2 `product_id`s appear twice (P0030, P0140: a current and an "(old)" row with different mrp) | `mrp` **−203**…883 · `cost_price` 6.79…631.79; **5 products cost > mrp** | `launch_date` 2023-01-03 → 2025-09-20 (clean) | `category`: 6 valid + 3 blank · `unit`: pack/unit/kg · `is_perishable`: True/False |
| `stores.csv` | 15 | `zone` 1 (S07 BTM Layout) | none | `capacity_orders_per_day` **0** for S12 (it did 135 orders) | `open_date` clean | `zone`: East 4, West 4, North 3, South 3, blank 1 |
| `orders_2026-10-01_increment.csv` | 160 | none | none | all within range | 2026-09-01 → 2026-10-01 | DELIVERED 150 (new, all 1 Oct), RETURNED 10 (corrections to Sept orders) |

### 0.2 Referential integrity

| Relationship | Orphans | Detail |
|---|---|---|
| `orders.customer_id` → `customers` | **18** | all 18 are *blank* customer_id (same rows as the completeness failure) |
| `orders.product_id` → `products` | **9** | fake ids `P9455, P9212, P9816, P9893, P9989 …` |
| `orders.store_id` → `stores` | **5** | `S99` ×1, `S17` ×3, `S00` ×1 |
| `payments.order_id` → `orders` | **8** | `PAY0002217–PAY0002224`: eight SUCCESS UPI payments to non-existent orders `ORD9xxxxx`, all time-stamped `2026-09-18 14:22:10` — looks like a gateway test batch / ghost payments |
| Orders with no payment | 184 | 156 CANCELLED (expected), **27 DELIVERED** (revenue with no cash), 1 RETURNED |
| Orders with 2 payment rows | 4 | identical to the 4 duplicated `payment_id`s — pure duplicates |
| Increment file → masters | 0 | clean |

### 0.3 Business-rule checks

| Rule | Result |
|---|---|
| `order_amount = qty × price × (1 − disc/100)` | **18 mismatches**: 15 rows are exactly **10×** the expected value (e.g. ORD002305 6757.5 vs 675.75 — decimal shift), 3 rows have `discount_pct` 120/150/250 so expected goes negative |
| Valid `order_status` | 5 bad: `Pending??` ×3 (unknown state), `delivered` ×2 (casing) |
| Valid `payment_mode` | 10 bad in orders, 10 bad in payments: `upi`, `UPI `, `CARD` (casing/space → fixable), `Cashh`, `cod` (typos → mappable to Cash on Delivery) |
| `quantity > 0` | **10 rows ≤ 0** (4 zeros, 6 negatives), all DELIVERED; negatives produce negative `order_amount` and negative `amount_paid` |
| Delivery 8–35 min; DELIVERED must have minutes | 6 rows with 480 / 900 / 1440 minutes (8 h, 15 h, 24 h); 8 DELIVERED rows with blank minutes; 72 non-DELIVERED rows (RETURNED/Pending) carry minutes — acceptable, returns were delivered first |
| SUCCESS payment = order_amount | **14 mismatches**, every one *short* by 10–100 ₹ (e.g. PAY0000779 paid 2.6 vs order 102.6; sum of gaps ₹785). Plus **44 DELIVERED orders whose only payment is FAILED** and **27 DELIVERED with no payment** |
| `mrp > 0`, `cost_price ≤ mrp` | P0109 mrp −203 (every order of P0109 fails "unit_price ≤ mrp"); P0019, P0073, P0074, P0117 sell below cost |
| No future dates | 7 orders, 7 payments, 3 customers dated after 2026-10-20 (2026-10-21, 2026-11-02, 2027-01-01, 2027-01-05) |
| Increment file | 150 new orders (1 Oct) + 10 existing Sept orders whose **only** change is `DELIVERED → RETURNED` — exactly the MERGE scenario |
| Store capacity | S12 has `capacity_orders_per_day = 0` yet 135 orders in September → every day "over capacity"; all other stores well under capacity |
| Customers | 12 malformed emails (`N/A`, missing `@`, missing TLD), 7 short phones, 5 bad pincodes, 3 signups *after* the customer's first order, 319 orders by `is_active = N` customers |

### 0.4 Data Issues Log

Severity legend — **Critical**: row is quarantined (ETL) / `dq_status = FAILED` and excluded from marts (ELT). **Warning**: row loads, issue logged in `dq_results`. **Info**: observation for the client, no rule.

| # | File | Row key (examples) | Column | Problem | Quality dimension | Proposed rule | Severity |
|---|---|---|---|---|---|---|---|
| 1 | orders.csv | ORD001462, ORD001441, ORD001692, ORD002129, ORD001239, ORD000783, ORD000539, ORD001434, ORD001326, ORD001549, ORD001870, ORD001110 | order_id | 12 order_ids appear twice as exact duplicate rows (24 rows) → double-counted revenue | Uniqueness | Keep first occurrence; quarantine the second with reason `duplicate_order_id` | Critical |
| 2 | orders.csv | ORD002255, ORD000701, ORD000997, ORD000580, ORD002019 (18) | customer_id | Blank customer_id; cannot join to customer | Completeness / Consistency | `customer_id` not null and exists in customers | Critical |
| 3 | orders.csv | ORD000986, ORD001058, ORD001685, ORD000070, ORD000789 (9) | product_id | References product ids P9xxx not in catalogue | Consistency | FK check against deduped products | Critical |
| 4 | orders.csv | ORD001799, ORD001046, ORD001492, ORD001275, ORD002126 (5) | store_id | Store S99 / S17 / S00 not in store master | Consistency | FK check against stores | Critical |
| 5 | orders.csv | ORD000403 (`N/A`), ORD000362 (blank), ORD001610 & ORD000534 (`2026-13-05`), ORD002012 & ORD000946 (`31/09/2026 10:00`) | order_ts | Missing or unparseable timestamp (month 13, Sept 31, dd/mm format) | Validity / Completeness | `order_ts` must parse as `YYYY-MM-DD HH:MM:SS` | Critical |
| 6 | orders.csv | ORD002119, ORD001765, ORD001725 (2027-01-01), ORD001693, ORD001089 (2026-10-21), ORD001121, ORD001965 (2026-11-02) | order_ts | Future-dated orders in a September export | Timeliness | `order_ts <= run time` | Critical |
| 7 | orders.csv | ORD000500, ORD001970, ORD001637 | order_status | Value `Pending??` not in allowed list | Validity | status ∈ {DELIVERED, CANCELLED, RETURNED} after trim/upper | Critical |
| 8 | orders.csv | ORD001066, ORD000153 | order_status | `delivered` lower-case | Validity | Standardise with `UPPER(TRIM())` before the check | Warning (auto-fixed) |
| 9 | orders.csv | ORD001812, ORD001414, ORD000159, ORD001364, ORD000433 (10) | payment_mode | `upi`, `UPI `, `CARD`, `Cashh`, `cod` | Validity | Map {upi, UPI → UPI; CARD → Card; cod, Cashh → Cash on Delivery}; anything unmapped → Critical | Warning (mapped) |
| 10 | orders.csv | ORD000469, ORD001724, ORD000099, ORD002360 (qty 0); ORD001296, ORD001459, ORD000867, ORD000462, ORD001124, ORD001384 (negative) | quantity | Quantity ≤ 0 on DELIVERED orders → zero or negative revenue | Accuracy | `quantity > 0` | Critical |
| 11 | orders.csv | ORD002305, ORD002004, ORD000845, ORD000964 … (15) | order_amount | Amount is exactly 10× the formula (decimal shift) | Accuracy | `abs(order_amount − formula) ≤ 0.01` | Critical |
| 12 | orders.csv | ORD002103, ORD001468, ORD001237 | discount_pct | Discount 250 / 150 / 120 % | Validity | `0 ≤ discount_pct ≤ 100` | Critical |
| 13 | orders.csv | ORD001554, ORD001800, ORD000731, ORD001277, ORD000326 (8) | delivery_minutes | DELIVERED but no delivery time | Completeness | DELIVERED ⇒ `delivery_minutes` not null | Critical |
| 14 | orders.csv | ORD001037, ORD002101, ORD000455, ORD001104, ORD000020 (6) | delivery_minutes | 480 / 900 / 1440 minutes (hours, not minutes) | Accuracy | `delivery_minutes < 120`; 35–120 → Warning | Critical |
| 15 | orders.csv | 69 RETURNED + 3 Pending rows | delivery_minutes | Non-DELIVERED rows carry delivery minutes | Consistency | None — a RETURNED order *was* delivered; keep for SLA stats | Info |
| 16 | orders.csv | 319 rows | customer_id | Orders from customers flagged `is_active = N` | Consistency | Log only; ask client (Q1 below) | Info |
| 17 | payments.csv | PAY0000509, PAY0001651, PAY0002000, PAY0000755 | payment_id | 4 payment_ids duplicated as exact rows (8 rows) → 4 orders look double-paid | Uniqueness | Keep first; quarantine duplicate | Critical |
| 18 | payments.csv | PAY0002217 … PAY0002224 | order_id | 8 SUCCESS UPI payments (₹4,580) for orders that do not exist, all stamped 2026-09-18 14:22:10 | Consistency | FK to orders; flag in reconciliation as `payment_without_order` | Critical |
| 19 | payments.csv | PAY0001568, PAY0000354, PAY0000779, PAY0000323, PAY0001167, PAY0001499, PAY0000909, PAY0000586 … (14) | amount_paid | SUCCESS payment short of order_amount by ₹10–100 | Consistency / Accuracy | Reconciliation row `amount_mismatch` with delta; payment still loads | Warning → reconciliation |
| 20 | payments.csv | PAY0001568, PAY0002175, PAY0000751, PAY0000944, PAY0000791, PAY0000737 (neg); PAY0000116, PAY0001111, PAY0001322, PAY0000954 (zero) | amount_paid | Payment ≤ 0 (mirrors negative/zero quantity orders) | Accuracy | `amount_paid > 0` for SUCCESS | Critical |
| 21 | payments.csv | PAY0002188, PAY0001850, PAY0001954, PAY0000817, PAY0001359 (10) | payment_mode | Same 5 variants as orders | Validity | Same mapping as #9 | Warning (mapped) |
| 22 | payments.csv | PAY0000396, PAY0000127, PAY0001908 (7) | payment_ts | Future-dated | Timeliness | `payment_ts <= run time` | Critical |
| 23 | payments.csv | 44 FAILED rows e.g. ORD000389, ORD000747, ORD000666 | payment_status | Order is DELIVERED but its only payment FAILED | Consistency | Reconciliation reason `delivered_but_payment_failed` | Warning → reconciliation |
| 24 | orders ↔ payments | ORD001113, ORD000737, ORD001398, ORD002078, ORD001570 (27) | order_id | DELIVERED order with no payment row at all (₹15,560 unbacked revenue) | Consistency | Reconciliation reason `delivered_without_payment` | Warning → reconciliation |
| 25 | payments.csv | 301 COD rows | gateway_ref | Blank gateway reference | Completeness | Allowed when `payment_mode = 'Cash on Delivery'`; otherwise Warning | Info |
| 26 | customers.csv | C00036, C00103, C00226, C00252, C00313 | customer_id | Each id has 2 rows with different phone / area (customer moved) | Uniqueness | Dedupe keeping last row; candidate for SCD-2 (bonus) | Critical (dedupe) |
| 27 | customers.csv | C00199, C00171, C00133, C00032, C00031 (8) | loyalty_tier | `BRONZE`, `bronze`, `SILVER` mixed casing | Consistency | `INITCAP(TRIM())` → Bronze/Silver/Gold/Platinum | Warning (fixed) |
| 28 | customers.csv | C00337, C00307, C00063, C00045, C00363 (12) | email | `N/A`, missing `@`, missing domain TLD | Validity | Regex `^[^@\s]+@[^@\s]+\.[a-z]{2,}$`; mask anyway | Warning |
| 29 | customers.csv | C00151, C00240, C00095, C00357, C00130 (7) | phone | 7–9 digits instead of 10 | Validity | Regex `^[6-9]\d{9}$`; mask anyway | Warning |
| 30 | customers.csv | C00132, C00083, C00166 (`5600XX`); C00395, C00342 (`000000`) | pincode | Not a valid 6-digit pincode | Validity | Regex `^5600\d{2}$` or `^\d{6}$` | Warning |
| 31 | customers.csv | C00355, C00394, C00233 (6) | city | Blank city (every other row is Bengaluru) | Completeness | Default to `Bengaluru`? ask client; Warning | Warning |
| 32 | customers.csv | C00400, C00398, C00038 | signup_date | Signup in the future (2026-11-12, 2026-12-20, 2027-01-05) — and after their first order | Timeliness / Consistency | `signup_date <= today` | Critical (future) |
| 33 | products.csv | P0030, P0140 | product_id | Two rows each: current and "(old)" name with different mrp | Uniqueness | Dedupe: keep the row whose name does not end in "(old)" | Critical (dedupe) |
| 34 | products.csv | P0051, P0060, P0068 | category | Blank category → margin-by-category report has an "Unknown" bucket | Completeness | `category` not null → quarantine product; orders of it fail FK | Critical |
| 35 | products.csv | P0109 | mrp | mrp = −203 (sign error; cost 162.5, orders sell at 203) | Accuracy | `mrp > 0` | Critical |
| 36 | products.csv | P0019, P0073, P0074, P0117 (+P0109) | cost_price | cost_price > mrp → guaranteed negative margin | Accuracy | `cost_price <= mrp` | Warning (ask client, may be loss-leaders) |
| 37 | stores.csv | S07 | zone | Blank zone (BTM Layout — geographically South) | Completeness | `zone` not null; ask client before defaulting | Critical for zone report |
| 38 | stores.csv | S12 | capacity_orders_per_day | Capacity 0 but 135 orders in Sept | Accuracy | `capacity > 0` | Warning |
| 39 | increment file | ORD000017, ORD000271, ORD000332, ORD000337, ORD000584, ORD000708, ORD000822, ORD001025, ORD001067, ORD001990 | order_status | Already present in orders.csv with DELIVERED; now RETURNED | Consistency (change history) | Upsert on order_id; keep raw history | Info — this is the designed MERGE case |

Count: **39 issues across all 6 files**, every one of the six quality dimensions represented.

### 0.5 Questions I would ask the client before building

1. **Negative / zero quantities (10 orders) and the matching negative payments** — are these returns booked into the order feed by mistake, data-entry errors, or genuine reversals? If reversals, finance should see them as negative revenue, not quarantine. My default: quarantine as Critical and show them in the quarantine report.
2. **Orders whose money never arrived** — 44 DELIVERED orders have only a FAILED payment, 27 have no payment at all, and 14 were paid ₹10–100 short. Should the 9 AM "net revenue" count what was *ordered and delivered* (order feed) or what was *collected* (gateway)? This decides whether the reconciliation table is informational or actually gates the fact table.
3. **Master-data gaps that need a business owner, not a rule** — store S07 has no zone (BTM Layout → South?), S12 has capacity 0, three products have no category, P0109 has a negative MRP, and 4 products are priced below cost. Who owns these corrections, and until they are fixed should I default (e.g. S07 → South, `Unknown` category) or exclude those rows from zone/category reports?

*(Bonus question I'd also raise: "Pending??" orders — is Pending a real state we should add to the allowed list, and should the exported `payment_mode` typos `Cashh`/`cod` be fixed at source in the OMS?)*


---

## Part A — ETL pipeline (`dags/nammamart_etl_hosur_narahari.py`)

**Flow:** Extract → Validate → Quarantine → Transform → Load → Reconcile → Report (16 tasks).

```
extract_customers ─► validate_customers ─┐
extract_products  ─► validate_products  ─┼─► validate_orders ─► validate_payments ─► quarantine_summary
extract_stores    ─► validate_stores    ─┤          │                  │          └► write_dq_results
extract_orders    ──────────────────────┘          ▼                  ▼
extract_payments  ────────────────────────────► transform ─────────► load ─► reconcile ─► report
```

| Step | Where | What it does |
|---|---|---|
| Extract | 5 parallel tasks → `etl_steps.extract` | Reads each CSV as text (exactly as received), checks the column contract (fails loudly on schema drift), snapshots to `include/work/hosur_narahari/<run_id>/`. XCom carries only paths + counts. |
| Validate | 5 tasks, 3 waves → `standardise.py` + `quality.py` | Standardise first (trim, upper/Title case, `upi`/`UPI `/`CARD`/`cod`/`Cashh` → canonical), then run **65 rules** across the 6 dimensions. Dims in parallel; orders after dims (FK to *clean* dims); payments after orders. |
| Quarantine | inside each validate + `quarantine_summary` | Critical failures → `include/quarantine/hosur_narahari/<file>_failed_<run_id>.csv` (row as received + `failure_reason` + `dq_warnings`) and table `etl_quarantine` (raw row as JSON). `summary_<run_id>.json` per run. |
| Transform | `etl_steps.transform` (pandas) | Dedupe customers (keep last) / products (drop "(old)"), mask email/phone, cast types, join orders↔customer/product/store, derive `order_date`, `is_delivered`, `net_revenue`, `cogs`, `gross_margin`, `within_15_min`. Blank `zone`/`category` → `Unknown`. |
| Load | `warehouse.upsert` | Star schema: `etl_fact_orders` + `etl_dim_customer`, `etl_dim_product`, `etl_dim_store` (+ `etl_payments`). Dims fully replaced; fact **delete-by-`order_id` then insert** → idempotent and applies corrections. |
| Reconcile | `etl_steps.reconcile` | `etl_payment_reconciliation`: one row per mismatch with `reason` ∈ {amount_mismatch, delivered_without_payment, delivered_but_payment_failed, returned_without_refund, returned_but_not_refunded, payment_for_quarantined_order, payment_without_order, cancelled_but_payment_success, payment_mode_mismatch}. |
| Report | `etl_steps.report` | Builds `etl_daily_zone_revenue`, `etl_category_margin`, prints them + total revenue in the log. |

**Evidence (base run on `orders.csv`):**

| Metric | Value |
|---|---|
| Rows in → loaded → quarantined (orders) | 2,412 → 2,310 → 102 |
| Payments in → loaded → quarantined | 2,228 → 2,199 → 29 |
| Customers / products / stores quarantined | 5 (superseded duplicates) / 2 ("(old)" rows) / 0 |
| Delivered orders, net revenue, gross margin | 2,088 · ₹1,135,628.80 · ₹260,999.43 |
| Re-run (idempotency) | `etl_fact_orders: before=2310 updated=2310 inserted=0 after=2310` |
| Increment `{"orders_file": "orders_2026-10-01_increment.csv"}` | `before=2310 updated=10 inserted=150 after=2460`; Sept revenue → ₹1,131,435.10 |
| `dq_results` rows per run | 72 |
| Reconciliation mismatches | 156 |

**Design decisions worth flagging**
1. *Dimension attribute defects don't delete facts.* Blank `category`, `mrp ≤ 0`, future `signup_date`, blank `zone` are **Warning** (filled with `Unknown` / kept). The strict version cascaded ~70 perfectly valid orders into quarantine for a master-data gap; finance would lose revenue over a catalogue typo.
2. *Two FK reason codes.* `orders.product_id_in_master` = id never existed; `orders.product_row_quarantined` = id exists but its master row failed a Critical rule. Finance sees *why* an order is missing.
3. *Payments FK is incremental-aware:* known orders = today's raw orders ∪ orders already in the warehouse, so loading only the increment file doesn't reject 2,000 valid payments.
4. *Money problems are reconciled, not quarantined.* A short/failed/missing payment loads the order and lands in `etl_payment_reconciliation`; finance sees the gap instead of revenue silently disappearing.

**Run it**
```bash
scripts/astro_corp.sh start                       # or `astro dev start` on an open network
astro dev run dags test nammamart_etl_hosur_narahari
astro dev run dags test nammamart_etl_hosur_narahari --conf '{"orders_file": "orders_2026-10-01_increment.csv"}'
.venv/bin/python scripts/wh.py "select * from etl_category_margin"
```

---

## Part B — ELT pipeline (`dags/nammamart_elt_hosur_narahari.py`)

**Flow:** Extract → Load raw → Transform in the warehouse with SQL (20 tasks; every transformation is a `.sql` file in `include/sql/`, rendered with `$params` and executed verbatim by `elt_steps.py`).

```
raw_customers ─► stg_customers ─┐
raw_products  ─► stg_products  ─┼─► stg_orders ─► stg_payments ─► raw_increment ─► stg_merge_increment
raw_stores    ─► stg_stores    ─┤                                 (skipped unless increment_file set)
raw_orders    ─────────────────┘                                                  │
raw_payments  ──────────────────────────────────────────────────────────────────┘
      ▼
mart_daily_store_kpis ► mart_category_margin ► mart_delivery_sla ► mart_payment_reconciliation
   ► mart_payment_mode_revenue ► mart_top_customers ► mart_rejections ► compare_with_etl
```

| Layer | Tables | SQL | Rule |
|---|---|---|---|
| **Raw** | `raw_orders`, `raw_payments`, `raw_customers`, `raw_products`, `raw_stores` | `raw_load.sql` | `read_csv(all_varchar=true, parallel=false)` → every column text, plus `_loaded_at`, `_source_file`, `_run_id`, `_row_num` (position in file). **Append-only**: `INSERT … WHERE NOT EXISTS (same run_id+file)` — re-running a run_id is a no-op, a new run appends. Nothing is ever cleaned or deleted. The Python wrapper only checks the column contract and fails loudly if it changed. |
| **Staging** | `stg_orders`, `stg_payments`, `stg_customers`, `stg_products`, `stg_stores` + `stg_rejections` | `stg_<entity>.sql` | Batch = this run's rows for this file. CTE chain: standardise (trim, `NULLIF`, casing, payment-mode `CASE`) → `try_cast`/`try_strptime` → one boolean per rule (same names as the Python rules) → `failure_reason` (Critical) / `dq_warnings` (Warning) → `dq_status`. Failed rows → `stg_rejections` with the raw row as JSON. Survivors (one per key) **`MERGE INTO stg_x … WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`**. PII masked here (`email_masked`, `phone_masked`). Every check → `dq_results`. |
| **Mart** | `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_top_customers` (+ `mart_payment_mode_revenue`, `mart_rejections`) | `mart_*.sql` | `CREATE OR REPLACE TABLE … FROM stg_* WHERE dq_status='PASSED'`. Only DELIVERED counts as revenue. Rebuilt every run → idempotent by construction. `mart_daily_store_kpis` also carries the store-capacity check (bonus). |

**Incremental load (B.3).** Trigger with `{"increment_file": "orders_2026-10-01_increment.csv"}`:

| Observation | Value |
|---|---|
| `raw_orders` | 2,412 → 4,984 after a re-run + increment (append-only; `ORD000017` now has a DELIVERED row from `orders.csv` *and* a RETURNED row from the increment — full history) |
| `stg_orders` | 2,400 → **2,550 (+150 inserted, 10 updated in place)**; `COUNT(DISTINCT order_id) = COUNT(*)` — no duplicates |
| The 10 corrected orders | `order_status = RETURNED`, `_source_file = orders_2026-10-01_increment.csv`, `dq_status = PASSED` |
| September revenue in marts | ₹1,135,628.80 → ₹1,131,435.10 (the 10 returns no longer count) |

**Do the mart totals match ETL? (B.4)** Yes — `compare_with_etl` is the last task and prints it:

| September | ETL (`etl_fact_orders`) | ELT (`stg_orders` PASSED / marts) |
|---|---|---|
| Clean orders | 2,310 | 2,310 |
| Delivered orders | 2,088 | 2,088 |
| Net revenue | ₹1,135,628.80 | ₹1,135,628.80 |
| Gross margin | ₹260,999.43 | ₹260,999.43 |
| Reconciliation rows | 156 | 156 |
| Rejected rows | 138 (etl_quarantine) | 138 (stg_rejections) |

They match because both pipelines implement the **same rule set with the same names** (`quality.py` ↔ `stg_*.sql`), the same dedupe survivors (first `order_id`; *last* `customer_id`; non-"(old)" product), the same canonical mappings and the same "DELIVERED only" revenue definition. The one time they *didn't* match was deliberate: after loading the increment into ELT only, `compare_with_etl` printed `*** MISMATCH ***` (ELT Sept ₹1,131,435.10 vs ETL ₹1,135,628.80) — exactly the 10 orders returned on 1 Oct. Feeding the same file to ETL (`updated=10 inserted=150`) made them agree again. That is the expected, explainable difference: *the two warehouses only match when they have consumed the same files.*

**Where ETL and ELT differ structurally (visible in the warehouse)**
- ELT keeps the bad rows: `stg_orders` has 2,400 rows = 2,310 PASSED + 90 FAILED survivors (12 duplicates exist only in `stg_rejections`). ETL's fact table has 2,310 — the bad rows live in files/`etl_quarantine`.
- ELT raw tables grow on every run (by design; audit/replay); ETL's intermediate files are per-run and disposable.
- In ELT a rule change is a SQL edit + re-run of staging over the already-loaded raw; in ETL it needs the source files re-read.

**Run it**
```bash
astro dev run dags test nammamart_elt_hosur_narahari
astro dev run dags test nammamart_elt_hosur_narahari --conf '{"increment_file": "orders_2026-10-01_increment.csv"}'
.venv/bin/python scripts/wh.py "select * from mart_payment_mode_revenue"
.venv/bin/python scripts/wh.py "select order_id, order_status, _source_file from raw_orders where order_id='ORD000017'"   # history
```


---

## Part C — Scheduling both DAGs for 9:00 AM IST, Monday–Saturday

Both DAG files carry the same block (see the comment header in each):

```python
SCHEDULE   = "0 9 * * 1-6"                                   # 09:00 Mon–Sat, in the start_date's timezone
START_DATE = pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata")  # timezone-aware → cron is evaluated in IST
default_args = {"owner": "hosur_narahari", "retries": 2, "retry_delay": timedelta(minutes=1),
                "execution_timeout": timedelta(minutes=10)}
@dag(schedule=SCHEDULE, start_date=START_DATE, catchup=False, dagrun_timeout=timedelta(minutes=20), max_active_runs=1, ...)
```

**What the scheduler recorded after the change** (from the Airflow metadata DB, `dag` and `dag_run` tables):

| | `nammamart_etl_hosur_narahari` | `nammamart_elt_hosur_narahari` |
|---|---|---|
| `timetable_summary` | `0 9 * * 1-6` | `0 9 * * 1-6` |
| `next_dagrun` | **2026-10-21 03:30:00+00** (= Wed 09:00 IST) | **2026-10-21 03:30:00+00** |
| Catch-up runs created when the schedule was applied (22:00 IST, Tue 20 Oct) | exactly **one**: `scheduled__2026-10-20T03:30:00+00:00` → success in 11.7 s | exactly **one**: `scheduled__2026-10-20T03:30:00+00:00` → success in 19.4 s |

Both scheduled runs started in the same second (16:34:07 UTC). Eight tasks (`raw_*`, `stg_customers`, `stg_products`, `validate_payments`, `reconcile`) logged `lock busy, waiting…` because the two DAGs share one DuckDB file — all waited a second or two and succeeded, no retry was consumed.

### Answers

**1. What is your cron expression?**
`0 9 * * 1-6` — minute 0, hour 9, any day-of-month, any month, days 1–6 (Monday–Saturday). Airflow evaluates it in the timezone of `start_date`, which is `Asia/Kolkata`, so it means 09:00 IST. *Justifications:* `catchup=False` because every export is a full snapshot (`orders.csv` is the whole month) — replaying each missed 9 AM would reload identical files N times and give finance nothing new; the latest slot is all that matters. `retries=2` with `retry_delay=1 min` absorbs the two realistic transient failures (a late-landing file, or the other DAG holding the DuckDB lock) while still finishing by ~09:05. `execution_timeout=10 min` per task and `dagrun_timeout=20 min` make a hang fail loudly inside the SLA window rather than at 09:30 with nothing to show.

**2. What time does the run show in UTC in the Airflow UI?**
**03:30 UTC.** IST is UTC+5:30 with no daylight saving, so 09:00 IST is always 03:30 UTC. The UI's *Next run* / `logical_date` for both DAGs reads `2026-10-21 03:30:00+00:00`, and the run IDs are `scheduled__2026-10-20T03:30:00+00:00`. (Airflow stores and displays everything in UTC; the IST intent lives in the DAG's timezone.)

**3. What would happen at 9 AM if you had left the `start_date` in UTC?**
Nothing — at 09:00 IST the scheduler would be idle. A naive/UTC `start_date` makes Airflow read `0 9 * * 1-6` as 09:00 **UTC**, which is **14:30 IST**. The run would fire five and a half hours after Kavya's 09:30 stand-up, every day, with no error anywhere: the DAG is green, the schedule looks right, and the numbers are simply never there in time. (Day-of-week would still line up because 14:30 IST is the same calendar day; had the slot been before 05:30 IST, the UTC date would also shift and Saturday's run would land on Friday.) This is the classic silent timezone bug, and the reason the timezone is explicit in `pendulum.datetime(..., tz="Asia/Kolkata")`.

**4. What happens if your laptop or Docker is off at 9 AM?**
No run happens at 9 — there is no scheduler alive to create it, so the 09:30 meeting has no fresh numbers. When Docker comes back, the scheduler notices the most recent slot (`2026-10-20T03:30`) has no run and, because `catchup=False`, creates **one** run for that slot and executes it immediately — this is exactly what happened above when the schedule was applied at 22:00 IST: one `scheduled__2026-10-20T03:30` run per DAG, not runs for 1–19 October. If the laptop had been off for a week, still only the latest slot would run (correct for full-snapshot files). The honest conclusion for Sameer: a laptop is not a production scheduler. The pipeline should run on an always-on host (a small VM with `astro dev start`, or a managed Airflow deployment), and Part F adds a `pipeline_runs` audit row plus a failure alert so that "no run by 09:05" is noticed rather than discovered in the meeting.

**Screenshots to capture for the submission** (UI at `http://…localhost:6563`): DAGs list showing *Schedule* `0 9 * * 1-6` and *Next run* `2026-10-21, 03:30:00 UTC`; the Grid view of either DAG showing the single `scheduled__2026-10-20T03:30:00+00:00` run in green.


---

## Part D — Data quality dimensions

### D.1 How a check works in each pipeline

| | ETL (Python) | ELT (SQL) |
|---|---|---|
| Where the rules live | `include/nammamart/quality.py` — one `Check(name, dimension, severity, fn)` per rule, grouped per file in `CHECKS` | `include/sql/stg_<entity>.sql` — one boolean column `f_<rule>` per rule in the `checks` CTE |
| Standardise before checking | `standardise.py` (trim, casing, payment-mode map) | first CTE `src` (`trim`, `NULLIF`, `upper`, `CASE` map) |
| **Critical** failure | row → quarantine CSV + `etl_quarantine`; never reaches `etl_fact_orders`/dims | row → `stg_rejections` (raw row as JSON); survivor kept in `stg_*` with `dq_status='FAILED'`; marts filter `dq_status='PASSED'` |
| **Warning** failure | row loads; reason kept in the fact/dim row's `dq_warnings`; counted in `dq_results` | row loads with `dq_status='PASSED'`; reason kept in `dq_warnings`; counted in `dq_results` |
| Dataset-level checks (freshness, volume) | `row_level=False` checks return 0/1 | aggregate `CASE … THEN 1 ELSE 0` rows in the `dq_results` insert |
| `dq_results` writer | task `write_dq_results` (`warehouse.write_dq_results`) | the `INSERT INTO dq_results … UNION ALL` block at the end of each `stg_*.sql` |
| Rule names | **identical in both** (`orders.quantity_positive`, …) so one scorecard covers both pipelines |

**`dq_results`** (shared by both DAGs; created by `warehouse.DQ_RESULTS_DDL` / `stg_common_ddl.sql`):

| column | meaning |
|---|---|
| `run_id`, `dag_id` | which run of which pipeline (`nammamart_etl_…` or `nammamart_elt_…`) |
| `check_name` | `<file>.<rule>`; the increment batch is prefixed `orders_increment.` |
| `dimension` | Completeness · Uniqueness · Validity · Accuracy · Consistency · Timeliness |
| `severity` | Critical (quarantine) · Warning (load + log) |
| `rows_checked`, `rows_failed` | batch size and failures (0/1 for dataset-level checks) |
| `status` | `PASS` (0 failed) · `FAIL` (Critical, >0) · `WARN` (Warning, >0) |
| `checked_at` | IST wall-clock |

Writes are idempotent per `(run_id, dag_id, check prefix)` — clearing and re-running a task replaces its rows rather than appending.

### D.2 Coverage — checks per dimension × file (identical in ETL and ELT)

Output of `.venv/bin/python scripts/dq_scorecard.py`, latest run of each DAG:

| Dimension | customers | orders | payments | products | stores | Critical | Warning |
|---|---|---|---|---|---|---|---|
| Completeness | 4 | 4 | 4 | 3 | 2 | 11 | 6 |
| Uniqueness | 1 | 1 | 1 | 2 | 1 | 6 | 0 |
| Validity | 5 | 5 | 3 | 2 | 1 | 8 | 8 |
| Accuracy | 1 | 5 | 1 | 3 | 1 | 6 | 5 |
| Consistency | 2 | 7 | 3 | 1 | 1 | 8 | 6 |
| Timeliness | 1 | 3 | 2 | 1 | 1 | 4 | 4 |
| **Total: 72 checks per pipeline per run** | 14 | 25 | 14 | 12 | 7 | 43 | 29 |

**Parity:** `checks in ETL: 72, in ELT: 72, common: 72, identical rows_failed: 72, mismatches: 0`. Per-run totals are the same for every run so far: 143 Critical rows failed, 82 Warning rows failed, 20 checks FAIL, 17 WARN, 35 PASS.

### D.3 The rule catalogue (generated from `quality.py`; the SQL mirrors it name for name)

| # | Check (`dq_results.check_name`) | Dimension | Severity | Rule | Rows failed (base run) |
|---|---|---|---|---|---|
| 1 | `orders.order_id_not_null` | Completeness | **Critical** | order_id present | 0 |
| 2 | `orders.customer_id_not_null` | Completeness | **Critical** | customer_id present | 18 |
| 3 | `orders.order_ts_not_null` | Completeness | **Critical** | order_ts present | 2 |
| 4 | `orders.delivered_has_delivery_minutes` | Completeness | **Critical** | DELIVERED ⇒ delivery_minutes not null | 8 |
| 5 | `orders.order_id_unique` | Uniqueness | **Critical** | order_id seen once (keep first) | 12 |
| 6 | `orders.order_ts_parses` | Validity | **Critical** | strict `YYYY-MM-DD HH:MM:SS` | 4 |
| 7 | `orders.order_status_in_list` | Validity | **Critical** | ∈ {DELIVERED, CANCELLED, RETURNED} after UPPER/TRIM | 3 |
| 8 | `orders.payment_mode_in_list` | Validity | **Critical** | ∈ {UPI, Card, Cash on Delivery, Wallet} after mapping | 0 |
| 9 | `orders.discount_pct_0_100` | Validity | **Critical** | 0 ≤ discount ≤ 100 | 3 |
| 10 | `orders.channel_in_list` | Validity | **Warning** | ∈ {App, Web} | 0 |
| 11 | `orders.quantity_positive` | Accuracy | **Critical** | quantity > 0 | 10 |
| 12 | `orders.unit_price_positive` | Accuracy | **Critical** | unit_price > 0 | 0 |
| 13 | `orders.order_amount_matches_formula` | Accuracy | **Critical** | |amount − qty×price×(1−disc/100)| ≤ 0.01 | 18 |
| 14 | `orders.delivery_minutes_below_120` | Accuracy | **Critical** | delivery_minutes < 120 | 6 |
| 15 | `orders.delivery_minutes_in_normal_range` | Accuracy | **Warning** | 8 ≤ minutes ≤ 35 | 0 |
| 16 | `orders.customer_id_in_master` | Consistency | **Critical** | customer exists in customers.csv | 0 |
| 17 | `orders.product_id_in_master` | Consistency | **Critical** | product exists in products.csv | 9 |
| 18 | `orders.store_id_in_master` | Consistency | **Critical** | store exists in stores.csv | 5 |
| 19 | `orders.customer_row_quarantined` | Consistency | **Critical** | customer exists but its row failed a Critical rule | 0 |
| 20 | `orders.product_row_quarantined` | Consistency | **Critical** | product exists but its row failed a Critical rule | 0 |
| 21 | `orders.store_row_quarantined` | Consistency | **Critical** | store exists but its row failed a Critical rule | 0 |
| 22 | `orders.unit_price_not_above_mrp` | Consistency | **Warning** | unit_price ≤ product mrp | 11 |
| 23 | `orders.order_ts_not_future` | Timeliness | **Critical** | order_ts ≤ run time | 7 |
| 24 | `orders.latest_order_not_stale` | Timeliness | **Warning** | newest (non-future) order_ts within 1 day of run | 1 |
| 25 | `orders.volume_not_anomalous` | Timeliness | **Warning** | ≥ 500 order rows received | 0 |
| 26 | `payments.payment_id_not_null` | Completeness | **Critical** | payment_id present | 0 |
| 27 | `payments.order_id_not_null` | Completeness | **Critical** | order_id present | 0 |
| 28 | `payments.amount_paid_not_null` | Completeness | **Critical** | amount_paid numeric | 0 |
| 29 | `payments.gateway_ref_present_for_non_cod` | Completeness | **Warning** | gateway_ref present unless COD | 0 |
| 30 | `payments.payment_id_unique` | Uniqueness | **Critical** | payment_id seen once | 4 |
| 31 | `payments.payment_ts_parses` | Validity | **Critical** | strict timestamp | 0 |
| 32 | `payments.payment_status_in_list` | Validity | **Critical** | ∈ {SUCCESS, FAILED, REFUNDED} | 0 |
| 33 | `payments.payment_mode_in_list` | Validity | **Critical** | ∈ allowed modes after mapping | 0 |
| 34 | `payments.success_amount_positive` | Accuracy | **Critical** | SUCCESS ⇒ amount_paid > 0 | 10 |
| 35 | `payments.order_id_in_orders` | Consistency | **Critical** | order exists in any orders file ever received | 8 |
| 36 | `payments.mode_matches_order` | Consistency | **Warning** | payment_mode = order's payment_mode | 0 |
| 37 | `payments.success_amount_equals_order_amount` | Consistency | **Warning** | SUCCESS ⇒ amount_paid = order_amount | 14 |
| 38 | `payments.payment_ts_not_future` | Timeliness | **Critical** | payment_ts ≤ run time | 7 |
| 39 | `payments.latest_payment_not_stale` | Timeliness | **Warning** | newest payment within 1 day | 1 |
| 40 | `customers.customer_id_not_null` | Completeness | **Critical** | customer_id present | 0 |
| 41 | `customers.full_name_not_null` | Completeness | **Warning** | full_name present | 0 |
| 42 | `customers.city_not_null` | Completeness | **Warning** | city present | 6 |
| 43 | `customers.email_not_null` | Completeness | **Warning** | email present | 4 |
| 44 | `customers.customer_id_unique` | Uniqueness | **Critical** | customer_id once (keep LAST = current address) | 5 |
| 45 | `customers.email_format` | Validity | **Warning** | regex `local@domain.tld` | 8 |
| 46 | `customers.phone_10_digits` | Validity | **Warning** | regex `[6-9]\d{9}` | 7 |
| 47 | `customers.pincode_6_digits` | Validity | **Warning** | regex `[1-9]\d{5}` | 5 |
| 48 | `customers.is_active_Y_N` | Validity | **Warning** | ∈ {Y, N} | 0 |
| 49 | `customers.signup_date_parses` | Validity | **Critical** | strict `YYYY-MM-DD` | 0 |
| 50 | `customers.loyalty_tier_in_list` | Consistency | **Critical** | ∈ {Bronze, Silver, Gold, Platinum} after Title-case | 0 |
| 51 | `customers.loyalty_tier_casing_was_fixed` | Consistency | **Warning** | raw tier differed from canonical casing | 8 |
| 52 | `customers.signup_date_not_future` | Timeliness | **Warning** | signup_date ≤ run date | 3 |
| 53 | `customers.signup_not_after_first_order` | Accuracy | **Warning** | signup_date ≤ customer's first order | 3 |
| 54 | `products.product_id_not_null` | Completeness | **Critical** | product_id present | 0 |
| 55 | `products.category_not_null` | Completeness | **Warning** | category present (else → 'Unknown') | 3 |
| 56 | `products.product_name_not_null` | Completeness | **Critical** | product_name present | 0 |
| 57 | `products.superseded_old_row` | Uniqueness | **Critical** | duplicate id whose name ends in "(old)" | 2 |
| 58 | `products.product_id_unique` | Uniqueness | **Critical** | product_id once (keep non-old, first) | 2 |
| 59 | `products.is_perishable_bool` | Validity | **Warning** | ∈ {True, False} | 0 |
| 60 | `products.launch_date_parses` | Validity | **Warning** | strict date | 0 |
| 61 | `products.mrp_positive` | Accuracy | **Warning** | mrp > 0 | 1 |
| 62 | `products.cost_price_positive` | Accuracy | **Critical** | cost_price > 0 | 0 |
| 63 | `products.cost_price_not_above_mrp` | Accuracy | **Warning** | cost_price ≤ mrp | 5 |
| 64 | `products.category_in_known_list` | Consistency | **Warning** | ∈ the 6 known categories | 0 |
| 65 | `products.launch_date_not_future` | Timeliness | **Critical** | launch_date ≤ run date | 0 |
| 66 | `stores.store_id_not_null` | Completeness | **Critical** | store_id present | 0 |
| 67 | `stores.zone_not_null` | Completeness | **Warning** | zone present (else → 'Unknown') | 1 |
| 68 | `stores.store_id_unique` | Uniqueness | **Critical** | store_id once | 0 |
| 69 | `stores.pincode_6_digits` | Validity | **Warning** | regex 6 digits | 0 |
| 70 | `stores.capacity_positive` | Accuracy | **Warning** | capacity_orders_per_day > 0 | 1 |
| 71 | `stores.zone_in_list` | Consistency | **Warning** | ∈ {North, South, East, West} | 0 |
| 72 | `stores.open_date_not_future` | Timeliness | **Critical** | open_date ≤ run date | 0 |


### D.4 Why these severities

- **Critical = the row itself is wrong or unusable for revenue.** Missing/invalid key, duplicate, unparseable or future timestamp, impossible quantity/amount/delivery time, status outside the agreed list, FK to a non-existent master. Any one of these would put a wrong number in the CFO's total, so the row is set aside with its reason (acceptance criteria 2 and 4).
- **Warning = the row is real but an attribute is doubtful.** Malformed email/phone/pincode (we mask them anyway), tier casing (we fix it), blank `city`/`zone`/`category` (filled with `Unknown` so the revenue stays visible and the gap is *shown* in the report instead of hidden), `cost_price > mrp` (a pricing question for the client, not a data error), delivery 35–119 min (slow, not impossible). Short/failed/missing payments are Warnings on the payment and land in the reconciliation table — finance wants to *see* ₹16.7 k of unbacked revenue, not have it vanish.
- **Deviation from the suggested table, and why:** `products.category_not_null`, `products.mrp_positive`, `customers.signup_date_not_future` and `stores.zone_not_null` are Warnings here, not Critical. Making them Critical in the first iteration cascaded ~70 valid orders into quarantine (their product/customer master row was dropped). A master-data typo should not delete a day's sales; it should show up as an `Unknown` bucket that someone has to explain. The cascade is still visible when it does happen: `orders.*_row_quarantined` reasons exist for exactly that case.
- **Timeliness "stale" check will always WARN on this dataset** (newest real order is 30 Sep, run date is Oct): that is the correct answer — the September export *is* stale on a 20 Oct run — and it is Warning, not Critical, because an old-but-complete file is still worth loading.

### D.5 Scorecard queries (used in Part F and for the client's "did today work?" question)

```sql
-- today's scorecard, both pipelines
SELECT dag_id, dimension, severity, status, COUNT(*) checks, SUM(rows_failed) rows_failed
FROM dq_results WHERE run_id = :run_id GROUP BY ALL ORDER BY 1,2,3,4;

-- trend of Critical failures per run (the "quality over time" view)
SELECT dag_id, run_id, MIN(checked_at) checked_at,
       SUM(rows_failed) FILTER (WHERE severity='Critical') critical_rows_failed,
       COUNT(*) FILTER (WHERE status='FAIL') checks_failed
FROM dq_results GROUP BY 1,2 ORDER BY 3;

-- do ETL and ELT agree? (0 rows = yes)
SELECT a.check_name, a.rows_failed etl, b.rows_failed elt
FROM dq_results a JOIN dq_results b USING (check_name)
WHERE a.dag_id LIKE '%etl%' AND b.dag_id LIKE '%elt%' AND a.run_id = :etl_run AND b.run_id = :elt_run
  AND a.rows_failed <> b.rows_failed;
```
`scripts/dq_scorecard.py` runs all of these (coverage matrix, parity, non-passing checks, trend).


---

## Part E — Data governance

### E.1 Ownership

| Dataset | Owner (accountable) | Steward (day-to-day) | Where it shows in the pipeline |
|---|---|---|---|
| `orders.csv` | Head of Operations (Kavya, COO's org) | OMS product owner | `raw_orders`, `etl_fact_orders`, `stg_orders` |
| `payments.csv` | Head of Finance | Payments/gateway integration lead | `raw_payments`, `etl_payments`, `stg_payments`, reconciliation tables |
| `customers.csv` | Head of Growth/CRM, jointly with the DPO for personal data | CRM admin | `raw_customers` (unmasked), `etl_dim_customer`/`stg_customers` (masked) |
| `products.csv` | Category management | Catalogue team | `raw_products`, `etl_dim_product`, `stg_products` |
| `stores.csv` | Head of Operations | Store ops manager | `raw_stores`, `etl_dim_store`, `stg_stores` |
| Warehouse tables (`etl_*`, `stg_*`, `mart_*`, `dq_results`, `pipeline_runs`) | Data/engineering (the FDE team now, a NammaMart data lead later) | same | `include/warehouse_hosur_narahari.duckdb` |

**Who approves a rule change such as the 120-minute delivery limit?** The *business owner* of the rule approves the threshold (Ops for delivery time, Finance for payment tolerance, Category for `cost_price ≤ mrp`); the data team implements it. Mechanically the rule is one line in `quality.py` (`delivery_minutes_below_120`) and one line in `stg_orders.sql`, changed via a pull request that must touch both so ETL and ELT stay in parity; the PR description cites the approver, and `dq_results` from the next run is the proof of effect (rows_failed moves from 6 to whatever the new limit yields).

### E.2 Data dictionary — fact and dimension tables (ETL names; ELT `stg_*` equivalents in brackets)

**`etl_fact_orders`** (`stg_orders`, 2,312 rows at base state; grain = one order line)

| Column | Type | Meaning | Source | Allowed values / rule | PII |
|---|---|---|---|---|---|
| `order_id` | VARCHAR, PK | Order identifier | orders.csv | `ORD\d{6}`, unique | no |
| `order_ts` | TIMESTAMP | When the order was placed (IST) | orders.csv `order_ts` | strict `YYYY-MM-DD HH:MM:SS`, ≤ run time | no |
| `order_date` | DATE | Calendar day of `order_ts` | derived | — | no |
| `customer_id` | VARCHAR, FK | Buyer | orders.csv | exists in dim_customer | no (key only) |
| `store_id` | VARCHAR, FK | Fulfilling dark store | orders.csv | `S01`–`S15` | no |
| `product_id` | VARCHAR, FK | Item | orders.csv | exists in dim_product | no |
| `quantity` | INTEGER | Units | orders.csv | > 0 | no |
| `unit_price` | DOUBLE | Selling price per unit (₹) | orders.csv | > 0; ≤ mrp (warning) | no |
| `discount_pct` | DOUBLE | Discount percent | orders.csv | 0–100 | no |
| `order_amount` | DOUBLE | Line value after discount (₹) | orders.csv | = qty × price × (1 − disc/100) ± 0.01 | no |
| `payment_mode` | VARCHAR | Tender type | orders.csv (standardised) | UPI · Card · Cash on Delivery · Wallet | no |
| `order_status` | VARCHAR | Lifecycle state | orders.csv (upper-cased) | DELIVERED · CANCELLED · RETURNED | no |
| `channel` | VARCHAR | Ordering channel | orders.csv | App · Web | no |
| `delivery_minutes` | INTEGER | Minutes from order to hand-over | orders.csv | not null if DELIVERED; < 120 (8–35 normal) | no |
| `is_delivered` | BOOLEAN | `order_status = 'DELIVERED'` | derived | — | no |
| `net_revenue` | DOUBLE | `order_amount` if delivered else 0 | derived | ≥ 0 | no |
| `cogs` | DOUBLE | `quantity × cost_price` if delivered | derived (dim_product) | ≥ 0 | no |
| `gross_margin` | DOUBLE | `net_revenue − cogs` | derived | may be negative (loss-leader) | no |
| `within_15_min` | BOOLEAN | SLA flag, delivered orders only | derived | NULL if not delivered | no |
| `dq_warnings` | VARCHAR | `;`-joined Warning rule names | quality engine | rule names from Part D | no |
| `loaded_at`, `run_id` | TIMESTAMP, VARCHAR | Lineage: when / which run wrote the row | pipeline | — | no |
| `effective_date` | DATE | As-of date of the source file (from its name) | pipeline | NULL for undated snapshots | no |
| *(ELT only)* `dq_status`, `failure_reason`, `_source_file`, `_run_id`, `_row_num`, `_staged_at` | | staging keeps FAILED survivors and exact file/line lineage | | | no |

**`etl_dim_customer`** (`stg_customers`, 400 rows)

| Column | Type | Meaning | Source | Allowed / rule | PII |
|---|---|---|---|---|---|
| `customer_id` | VARCHAR, PK | Customer key | customers.csv | unique (last row wins) | no |
| `full_name` | VARCHAR | Customer name | customers.csv | — | **yes** (kept for top-customer report; see E.4) |
| `email_masked` | VARCHAR | `pr***@example.com` | customers.csv `email` | masked, never raw | yes (masked) |
| `phone_masked` | VARCHAR | `98******21` | customers.csv `phone` | masked, never raw | yes (masked) |
| `city`, `area`, `pincode` | VARCHAR | Location | customers.csv | pincode `[1-9]\d{5}` (warning) | quasi-identifier |
| `signup_date` | DATE | CRM signup | customers.csv | ≤ today (warning) | no |
| `loyalty_tier` | VARCHAR | Programme tier | customers.csv (Title-cased) | Bronze · Silver · Gold · Platinum | no |
| `is_active` | BOOLEAN | CRM active flag | `Y/N` | — | no |

**`etl_dim_product`** (`stg_products`, 150 rows): `product_id` PK · `product_name` · `category` (6 known values or `Unknown`) · `sub_category` · `brand` · `mrp` DOUBLE (> 0, warning) · `cost_price` DOUBLE (> 0; ≤ mrp warning) · `unit` (pack/unit/kg) · `is_perishable` BOOLEAN · `launch_date` DATE. No PII.

**`etl_dim_store`** (`stg_stores`, 15 rows): `store_id` PK (`S01`–`S15`) · `store_name` · `zone` (North/South/East/West/`Unknown`) · `area` · `pincode` · `open_date` DATE · `manager_name` VARCHAR (**employee PII**) · `capacity_orders_per_day` INTEGER (> 0, warning).

### E.3 Lineage — tracing one number

**The number:** `mart_daily_store_kpis` → `2026-09-01`, store `S04` (Jayanagar): `net_revenue = 1103.30`, `gross_margin = 55.06`, `delivered_orders = 1`.

**In ELT (three queries, no guessing):**
1. `stg_orders WHERE order_date='2026-09-01' AND store_id='S04' AND dq_status='PASSED'` → one row, `ORD000052`, `order_amount 1103.3`, `quantity 2`, `product_id P0068`, **`_source_file='orders.csv'`, `_row_num=2075`**, `_run_id='scheduled__2026-10-21T03:30:00+00:00'`.
2. `raw_orders WHERE order_id='ORD000052'` → the row exactly as received: `2026-09-01 17:20:19, C00157, S04, P0068, 2, 649.0, 15, 1103.3, Wallet, DELIVERED, App, 23`, `_row_num=2075` → `sed -n 2076p orders.csv` (header is line 1) prints the identical text.
3. The margin: `stg_products WHERE product_id='P0068'` → `cost_price 524.12`, **`products.csv` row 68** → 1103.30 − 2 × 524.12 = 55.06 ✓ (P0068 is one of the blank-category products, hence it sits in the `Unknown` bucket of `mart_category_margin`).

**In ETL:** `etl_fact_orders WHERE order_date='2026-09-01' AND store_id='S04'` → `ORD000052`, `net_revenue 1103.3`, `cogs 1048.24`, `gross_margin 55.06`, `run_id`, `loaded_at`. That tells me *which run* produced it and the arithmetic, but not the file line: I have to `grep -n '^ORD000052,' orders.csv` (line 2076) and trust that the current file is what that run read — the per-run `include/work/<run_id>/orders_raw.csv` snapshot exists only until it is cleaned up, and the pandas path has no row number.

**Which is easier, and why.** ELT, clearly. The lineage is *data*, not inference: `raw_*` keeps every row with `_source_file`/`_row_num`/`_run_id`, and staging carries those columns forward, so file → row → staged row → mart is a join. In ETL the raw bytes never enter the warehouse; lineage stops at `run_id`, and reconstructing "what did the file look like at 9 AM" depends on files outside the database. (The ETL quarantine files do keep as-received rows, but only for *rejected* rows.)

### E.4 Personal data

**Columns that are personal data:** `customers.full_name`, `customers.email`, `customers.phone` (direct identifiers); `customers.area` + `customers.pincode` + `signup_date` (quasi-identifiers — together they can single out a person in a 15-store city); `stores.manager_name` (employee personal data). Everything in `orders`/`payments` is transactional, but `customer_id` links to the person, so joined outputs inherit the sensitivity.

**How it was masked:** email keeps the first two characters of the local part and the full domain (`priya.hegde199@example.com` → `pr***@example.com`); phone keeps the first two and last two digits (`9774349021` → `97******21`). Identical logic in Python (`masking.py`) and SQL (`stg_customers.sql`), applied *before* any reporting table is written, so `etl_dim_customer`, `stg_customers`, `mart_top_customers` never contain a raw email or phone. `full_name` and `manager_name` are kept (the business asked for "top customers by name"); if the DPO disagrees they are one `CASE`/`map` away from `P. H***`.

**Why mask rather than delete:** the columns still carry analytic value — domain mix tells marketing which providers customers use, the masked shape lets support confirm "is this the 97…21 number?" without seeing it, and masked values remain joinable on `customer_id`. Deleting would also break the uniqueness check that caught five customers whose phone changed. Masking keeps utility and auditability while removing identifiability from every table analysts touch.

**Applicable law:** the **Digital Personal Data Protection Act, 2023 (DPDP Act)** and the DPDP Rules notified in 2025. NammaMart is a *Data Fiduciary*; customers are *Data Principals*. The Act expects: a lawful basis (consent or a legitimate use such as fulfilling the order) and purpose limitation — order data collected to deliver groceries should not be re-purposed for analytics without notice; **data minimisation** (hence masking in reporting tables); reasonable security safeguards and access control; notification of personal-data breaches to the Data Protection Board and affected principals; honouring principals' rights to access, correction and erasure (so we need to be able to find and delete `C00199` across raw, staging and marts); and retention only as long as the purpose requires. Penalties run to ₹250 crore per breach, which is why "spreadsheets with phone numbers passed around" is the first thing this pipeline removes.

### E.5 Access control

| Table family | Who may read | Why |
|---|---|---|
| `raw_customers` (unmasked email/phone/name) | Nobody by default. Break-glass only: the DPO or a named data engineer, for a logged incident/erasure request. Not Finance, not Ops, not analysts. | It is the only place raw PII exists; its purpose is replay and legal audit, not analysis. |
| other `raw_*` (orders, payments, products, stores) | Data engineering, internal audit, compliance | Needed for replay, lineage and "show me the row as received" |
| `stg_*`, `stg_rejections`, `etl_quarantine`, quarantine CSVs | Data engineering, data-quality stewards, Finance ops (reconciliation), internal audit | Contain FAILED rows and masked PII; useful to explain rejections, too noisy for dashboards |
| `mart_*`, `etl_fact_orders`, `etl_dim_*`, `etl_daily_zone_revenue`, `etl_category_margin` | Everyone in the ops/finance/leadership review; BI tools | Masked, validated, business-ready |
| `dq_results`, `pipeline_runs` | Everyone (read-only) | "Did today's run work?" should be answerable without asking engineering |

Implementation in this setup: DuckDB has no row-level grants, so the practical control is *who can open the file* (filesystem/Docker volume permissions) plus publishing the mart layer to a separate read-only database/export for BI, and keeping `raw_customers` out of any shared copy. In a managed warehouse this becomes schemas (`raw`, `staging`, `mart`) with grants per role.

### E.6 Retention

- **Raw** (`raw_*`): 7 years for `raw_orders`/`raw_payments` — Indian tax and company-law books-of-account requirements, and they are the audit trail for revenue; `raw_customers` **90 days**, then rows for customers who have erased/left are deleted and the rest re-masked in place — DPDP says keep personal data only as long as the purpose needs it, and the replay purpose is satisfied within a quarter.
- **Quarantine files / `stg_rejections` / `etl_quarantine`**: 180 days. Long enough for source teams to fix root causes and for a quarterly quality review; after that they are summarised into `mart_rejections`-style counts and the row-level files deleted (they contain unmasked raw rows, including the blank-customer orders).
- **Masked customer dimension**: lifetime of the customer relationship + 1 year, then anonymised (`customer_id` hashed, name dropped) — masked data is still personal data under DPDP if it can be linked back via `customer_id`.
- **Marts / facts**: 7 years (financial reporting), but they contain no direct identifiers. "After that" means a scheduled Airflow task that deletes/anonymises, writes the counts to `pipeline_runs`, and is itself audited — retention that is not automated is not retention.

### E.7 Secrets

Credentials live in Airflow's metadata database as **Variables/Connections** (encrypted with the Fernet key) and are read at run time with `Variable.get("SLACK_WEBHOOK_URL", default_var=None)` — exactly how the trainer's `slack_notifier.py` does it and how the Part F alert will. Locally they are set in the UI (Admin → Variables) or `airflow_settings.yaml`, which is **git-ignored**; the repo contains no key, password or URL. They must never sit in the DAG file or on GitHub because (a) a DAG file is read by every scheduler/worker and shown in the UI *Code* tab to anyone with DAG access, (b) git history is forever — a webhook committed once is leaked even after deletion, and public repos are scraped within minutes, and (c) a leaked Slack webhook lets anyone post as the pipeline, which is the perfect channel for a phishing message to Finance. Rotation is also trivial when the secret is a Variable and impossible when it is baked into code.

### E.8 Audit — "pick a rejected order"

Example: **`ORD001296`**.

| Question | ETL | ELT |
|---|---|---|
| Exactly as received | `include/quarantine/hosur_narahari/orders_failed_<run_id>.csv` line for `ORD001296`, or `etl_quarantine.raw_row` → `{"order_id":"ORD001296","order_ts":"2026-09-17 16:07:59","customer_id":"C00305","store_id":"S03","product_id":"P0046","quantity":"-2","unit_price":"197.0","discount_pct":"0","order_amount":"-394.0","payment_mode":"UPI","order_status":"DELIVERED","channel":"App","delivery_minutes":"35"}` | `raw_orders WHERE order_id='ORD001296'` → same values, `_source_file='orders.csv'`, `_row_num=800` (= line 801 of the file), untouched; `stg_rejections.raw_row` holds the same JSON |
| The rule it broke | `failure_reason = orders.quantity_positive` (Accuracy, Critical) | identical: `stg_rejections.failure_reason` and `stg_orders.dq_status='FAILED'` |
| Proof it is not in the CFO's total | `SELECT COUNT(*) FROM etl_fact_orders WHERE order_id='ORD001296'` → **0**; `etl_daily_zone_revenue` is built only from that table | `SELECT COUNT(*) FROM stg_orders WHERE order_id='ORD001296' AND dq_status='PASSED'` → **0**; every `mart_*` filters `dq_status='PASSED'` |
| Its payment | `PAY0000737` (−394, SUCCESS) is quarantined too (`payments.success_amount_positive`) and appears in `etl_payment_reconciliation` as `payment_for_quarantined_order` | same reason code in `mart_payment_reconciliation` |

Both answer finance, but ELT answers *inside SQL* (row → rejection → mart in three joins), while ETL needs the file system for the as-received row.

### E.9 Change history — DELIVERED on 3 Sep, RETURNED on 1 Oct

Concrete case from the increment: `ORD000017` (1 Sep, ₹16.00) and nine others. After the incremental load:

- **ETL**: the fact row is replaced (`updated=10`) → `order_status='RETURNED'`, `net_revenue=0`. September revenue drops from ₹1,135,628.80 to ₹1,131,435.10. The old DELIVERED version is gone from the warehouse; its only trace is the previous run's work snapshot and `run_id` on the new row.
- **ELT**: `stg_orders` is MERGED → same current view, September ₹1,131,435.10. But `raw_orders` still holds **both** rows: `DELIVERED` from `orders.csv` (row 1256) and `RETURNED` from `orders_2026-10-01_increment.csv` (row 1), each with `_loaded_at`. The history is queryable: `SELECT order_status, _source_file, _loaded_at FROM raw_orders WHERE order_id='ORD000017' ORDER BY _loaded_at`.

**Which answer does finance want?** Both, for different purposes. The *closed books* for September must not move after month-end — September revenue "as reported" stays ₹1,135,628.80 and the ₹16 return is booked as an October adjustment (a sales return in the period it happened). The *operational/current* view (how much of what we sold in September did we actually keep?) is ₹1,131,435.10. A single mutable fact table can only show one of them.

**How to keep both views:** (1) keep raw append-only (ELT already does) so any "as of date X" state can be rebuilt by replaying files loaded before X; (2) add an `order_status_history` table (or an SCD-2 fact with `valid_from/valid_to`) populated from raw on every run — one row per status change with the file that reported it — so `SUM(amount) WHERE status_as_of('2026-09-30')='DELIVERED'` is a query, not an archaeology project; (3) snapshot period-end marts (`mart_daily_store_kpis_2026_09` frozen after close). The guard added in this part (a stale snapshot can never regress RETURNED→DELIVERED) protects the *current* view; the history table protects the *as-reported* view.
