# NammaMart Assignment — Checkpoint Tracker

**Learner:** hosur_narahari | **Branch:** `hosur_narahari_assignment`
**Resume rule:** read this file first; continue from the first unchecked Part. Stop after each Part for review.

## Environment (done 2026-10-20)
- [x] Astro CLI 1.46.0 installed (`brew install astro`)
- [x] Local venv `.venv` (Python 3.12) with pandas + duckdb for profiling (`source .venv/bin/activate`)
- [x] Dataset copied to `include/data/nammamart/` (md5 verified, never edit)
- [x] Branch `hosur_narahari_assignment` created

## Conventions
- Name token: `hosur_narahari`
- DAGs: `dags/nammamart_etl_hosur_narahari.py`, `dags/nammamart_elt_hosur_narahari.py`
- Warehouse: `include/warehouse_hosur_narahari.duckdb`
- Quarantine: `include/quarantine/hosur_narahari/`
- Helpers: `include/nammamart/` (Python), `include/sql/` (SQL)
- Write-up: `SUBMISSION.md`

## Parts
- [x] **Part 0** — Discovery & data profiling → Data Issues Log (39 issues, all 6 files) + 3 client questions. Artifacts: `SUBMISSION.md` §Part 0, `include/nammamart/profiling/{profile_data.py,profile_report.md,issue_candidates.csv}`. **Awaiting user review.**
- [ ] **Part A** — ETL DAG (Extract → Validate → Quarantine → Transform → Load star schema → Reconcile → Report)
- [ ] **Part B** — ELT DAG (Raw → Staging → Mart, SQL files, MERGE incremental)
- [ ] **Part C** — Schedule both at 9:00 AM IST Mon–Sat (cron, pendulum tz, catchup, retries) + answers
- [ ] **Part D** — DQ checks across 6 dimensions in both pipelines, `dq_results` table
- [ ] **Part E** — Governance answers (9 questions)
- [ ] **Part F** — `pipeline_runs` audit + 10 observability experiments + 5 pillars
- [ ] **Part G** — One-page recommendation to CTO

## Notes / decisions
- Part 0 key findings that drive the pipeline design:
  - orders: 12 dup order_ids, 18 blank customer_id, 9 bad product FK, 5 bad store FK, 7 bad/unparseable order_ts, 7 future-dated, 10 qty<=0, 15 amounts 10x off, 3 discount>100, 8 DELIVERED w/o minutes, 6 minutes>120, status `Pending??`/`delivered`, payment_mode variants upi/UPI /CARD/Cashh/cod
  - payments: 4 dup payment_ids, 8 ghost payments to non-existent orders, 14 short payments, 10 amount<=0, 44 FAILED on DELIVERED, 27 DELIVERED w/o any payment, 7 future-dated
  - customers: 5 dup ids (phone/area differ → SCD2 candidate), tier casing, 12 bad emails, 7 bad phones, 5 bad pincodes, 6 blank city, 3 future signups
  - products: P0030/P0140 dup with "(old)" rows, 3 blank category, P0109 mrp=-203, 5 cost>mrp
  - stores: S07 blank zone, S12 capacity 0
  - increment: clean; 150 new + 10 DELIVERED→RETURNED corrections
