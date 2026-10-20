# NammaMart ETL & ELT Pipeline — Submission

**Learner:** Hosur Narahari (`hosur_narahari`) · **Branch:** `hosur_narahari_assignment`
**Module:** FDAIE Module 2 · Data Engineering Foundations

| Section | Status |
|---|---|
| Part 0 — Discovery & Data Issues Log | ✅ below |
| Part A — ETL pipeline | ✅ below |
| Part B — ELT pipeline | ✅ below |
| Part C — Scheduling answers | ⏳ |
| Part D — Data quality dimensions | ⏳ |
| Part E — Governance answers | ⏳ |
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
