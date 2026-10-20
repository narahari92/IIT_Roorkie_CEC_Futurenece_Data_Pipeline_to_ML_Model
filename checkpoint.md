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
- [x] **Part 0** — Discovery & data profiling → Data Issues Log (39 issues, all 6 files) + 3 client questions. Artifacts: `SUBMISSION.md` §Part 0, `include/nammamart/profiling/{profile_data.py,profile_report.md,issue_candidates.csv}`. Approved.
- [x] **Part A** — ETL DAG `dags/nammamart_etl_hosur_narahari.py` + helpers `include/nammamart/{config,quality,standardise,masking,warehouse,etl_steps}.py`. Verified: parses, green run in UI, idempotent (re-run → 0 inserts), increment via `--conf '{"orders_file": "orders_2026-10-01_increment.csv"}'` → updated=10 inserted=150. Approved.
- [x] **Part B** — ELT DAG `dags/nammamart_elt_hosur_narahari.py` + `include/nammamart/elt_steps.py` + 14 SQL files in `include/sql/` (raw_load, stg_common_ddl, stg_×5, mart_×7). Verified: green run, raw append-only, staging MERGE (+150/10 updated), marts match ETL exactly (Sept 2088 / 1,135,628.80 / 260,999.43). Increment via `--conf '{"increment_file": "orders_2026-10-01_increment.csv"}'`. **Awaiting user review.**
- [x] **Part C** — Both DAGs: `schedule="0 9 * * 1-6"`, `start_date=pendulum.datetime(2026,10,1,tz="Asia/Kolkata")`, `catchup=False`, `dagrun_timeout=20m`, default_args retries=2/1min/execution_timeout=10m. Verified next_dagrun = 2026-10-21 03:30 UTC; one catch-up run each; concurrent runs OK (8 lock waits, 0 failures). Answers in SUBMISSION.md §Part C. **Awaiting user review.**
- [ ] **Part D** — DQ checks across 6 dimensions in both pipelines, `dq_results` table
- [ ] **Part E** — Governance answers (9 questions)
- [ ] **Part F** — `pipeline_runs` audit + 10 observability experiments + 5 pillars
- [ ] **Part G** — One-page recommendation to CTO

## Environment workaround (corporate TLS proxy)
- Laptop runs Cato Networks TLS inspection → Astro image build fails on PyPI TLS. Fix: `scripts/astro_corp.sh start|restart|stop`
  builds `astro-runtime-corp-ca:3.3-2` (runtime + Cato CA from keychain, certs in git-ignored `.certs/`) then `nammamart-airflow:latest`,
  and starts astro with `--image-name nammamart-airflow:latest`. Trainer's `Dockerfile` untouched. Rebuild needed only when requirements.txt changes.
- Airflow 3.3.0 / Python 3.14 in the image → use `from airflow.sdk import dag, task, get_current_context`.
- Astro UI URL printed by `astro dev start` (reverse proxy, e.g. http://iit-roorkie-cec-futurenece-data-pipeline-to-ml-model.localhost:6563), admin/admin.

## Notes / decisions
- Part 0 key findings that drive the pipeline design:
  - orders: 12 dup order_ids, 18 blank customer_id, 9 bad product FK, 5 bad store FK, 7 bad/unparseable order_ts, 7 future-dated, 10 qty<=0, 15 amounts 10x off, 3 discount>100, 8 DELIVERED w/o minutes, 6 minutes>120, status `Pending??`/`delivered`, payment_mode variants upi/UPI /CARD/Cashh/cod
  - payments: 4 dup payment_ids, 8 ghost payments to non-existent orders, 14 short payments, 10 amount<=0, 44 FAILED on DELIVERED, 27 DELIVERED w/o any payment, 7 future-dated
  - customers: 5 dup ids (phone/area differ → SCD2 candidate), tier casing, 12 bad emails, 7 bad phones, 5 bad pincodes, 6 blank city, 3 future signups
  - products: P0030/P0140 dup with "(old)" rows, 3 blank category, P0109 mrp=-203, 5 cost>mrp
  - stores: S07 blank zone, S12 capacity 0
  - increment: clean; 150 new + 10 DELIVERED→RETURNED corrections
- Part A design decisions:
  - Dimension *attribute* defects (blank category, mrp<=0, future signup_date, blank zone) are Warning, not Critical — otherwise ~70 valid orders cascade into quarantine for a master-data gap. Category→'Unknown', zone→'Unknown'.
  - Orders FK: `*_in_master` = id absent from raw master (Critical); `*_row_quarantined` = id exists but master row failed Critical (Critical, explicit reason).
  - Payments FK compares to raw orders ∪ orders already in warehouse (fact + quarantine) so incremental runs don't mis-quarantine payments.
  - Fact load = delete-by-order_id + insert (idempotent + handles status corrections). Dims = full replace. etl_payments upserted on payment_id.
  - Quarantine: file per source per run (`include/quarantine/hosur_narahari/<file>_failed_<run_id>.csv`, rows as received + failure_reason) AND table `etl_quarantine` (raw_row JSON).
  - Intermediate data in `include/work/hosur_narahari/<run_id>/` (git-ignored); XCom carries only paths/counts.
  - Base-state totals (orders.csv only): fact 2310 rows, 2088 delivered, net revenue 1,135,628.80, gross margin 260,999.43; 138 quarantined (102 orders, 29 payments, 5 customers, 2 products); 156 reconciliation mismatches.
  - After increment: fact 2460, Sept revenue 1,131,435.10 (10 returns), Oct 79,652.20.
- Part B design decisions:
  - Raw: `INSERT … WHERE NOT EXISTS (run_id, source_file)` — append-only, re-run of same run_id is a no-op. `_row_num` added for lineage.
  - Staging: per-file batch (`_run_id`,`_source_file`) → TEMP `_batch` with one boolean per rule → failure_reason/dq_warnings/dq_status → `stg_rejections` (raw_row JSON) → MERGE survivors on key. Rules/names identical to quality.py.
  - stg_orders holds PASSED and FAILED survivors (2400 = 2310 + 90); marts filter dq_status='PASSED'. Duplicate rows only in stg_rejections.
  - Increment = `raw_increment` + `stg_merge_increment` tasks (skip via AirflowSkipException if `increment_file` param empty); marts use trigger_rule none_failed. dq check names for the increment batch are prefixed `orders_increment.`.
  - Marts chained sequentially (single DuckDB writer); connect() retries ~60 s on lock for the 5 parallel raw loads.
  - `compare_with_etl` final task prints Sept totals of both pipelines and `match` flag.
  - Reserved words bit me: `name`/`rows` can't be bare aliases in DuckDB.
  - `increment_file` Param is nullable (`type=["null","string"]`) so the UI trigger form accepts empty.
- Part C notes:
  - Both DAGs fire at the same instant; DuckDB single-writer handled by `warehouse.connect()` lock-wait (saw 8 `lock busy` waits, all succeeded).
  - Task logs live in the container at /usr/local/airflow/logs/dag_id=…/run_id=…/task_id=…; metadata via `docker exec <scheduler> psql postgresql://postgres:postgres@postgres:5432/postgres`.
