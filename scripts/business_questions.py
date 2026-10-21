#!/usr/bin/env python
"""The six business questions, answered from BOTH pipelines (September 2026). Prints SQL + result."""
import duckdb, pandas as pd
pd.set_option("display.width", 220); pd.set_option("display.max_columns", 30)
con = duckdb.connect("include/warehouse_hosur_narahari.duckdb", read_only=True)
Q = {
 "Q1 Daily net revenue by zone and store (delivered only, after discount) — shown as September totals": {
  "ETL": "SELECT zone, store_id, store_name, SUM(delivered_orders) delivered, ROUND(SUM(net_revenue),2) net_revenue FROM etl_daily_zone_revenue WHERE order_date BETWEEN '2026-09-01' AND '2026-09-30' GROUP BY ALL ORDER BY zone, net_revenue DESC",
  "ELT": "SELECT zone, store_id, store_name, SUM(delivered_orders) delivered, ROUND(SUM(net_revenue),2) net_revenue FROM mart_daily_store_kpis WHERE order_date BETWEEN '2026-09-01' AND '2026-09-30' GROUP BY ALL ORDER BY zone, net_revenue DESC"},
 "Q2 Revenue and gross margin by category": {
  "ETL": "SELECT p.category, ROUND(SUM(f.net_revenue),2) revenue, ROUND(SUM(f.gross_margin),2) gross_margin, ROUND(100*SUM(f.gross_margin)/SUM(f.net_revenue),2) margin_pct FROM etl_fact_orders f JOIN etl_dim_product p USING (product_id) WHERE f.is_delivered AND f.order_date <= '2026-09-30' GROUP BY 1 ORDER BY 2 DESC",
  "ELT": "SELECT p.category, ROUND(SUM(o.order_amount),2) revenue, ROUND(SUM(o.order_amount - o.quantity*p.cost_price),2) gross_margin, ROUND(100*SUM(o.order_amount - o.quantity*p.cost_price)/SUM(o.order_amount),2) margin_pct FROM stg_orders o JOIN stg_products p USING (product_id) WHERE o.dq_status='PASSED' AND o.order_status='DELIVERED' AND o.order_date <= '2026-09-30' GROUP BY 1 ORDER BY 2 DESC"},
 "Q3 Average delivery time per store and % within 15 minutes": {
  "ETL": "SELECT s.store_id, s.store_name, ROUND(AVG(f.delivery_minutes),2) avg_minutes, ROUND(100.0*AVG(f.within_15_min::INT),2) pct_within_15 FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id) WHERE f.is_delivered AND f.order_date <= '2026-09-30' GROUP BY ALL ORDER BY 4 DESC",
  "ELT": "SELECT store_id, store_name, avg_delivery_minutes, pct_within_15 FROM mart_delivery_sla ORDER BY pct_within_15 DESC"},
 "Q4 Revenue by payment mode and the total that does not reconcile": {
  "ETL": "WITH r AS (SELECT f.payment_mode, ROUND(SUM(ABS(x.delta)),2) unreconciled FROM etl_payment_reconciliation x JOIN etl_fact_orders f USING (order_id) WHERE x.reason IN ('delivered_without_payment','delivered_but_payment_failed','amount_mismatch') GROUP BY 1) SELECT f.payment_mode, ROUND(SUM(f.net_revenue),2) net_revenue, COALESCE(r.unreconciled,0) unreconciled FROM etl_fact_orders f LEFT JOIN r USING (payment_mode) WHERE f.is_delivered GROUP BY ALL ORDER BY 2 DESC",
  "ELT": "SELECT payment_mode, net_revenue, unreconciled_amount, unreconciled_pct FROM mart_payment_mode_revenue"},
 "Q5 Top 10 customers by spend with loyalty tier (no raw email/phone)": {
  "ETL": "SELECT c.customer_id, c.full_name, c.loyalty_tier, c.email_masked, c.phone_masked, ROUND(SUM(f.net_revenue),2) spend FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id) WHERE f.is_delivered GROUP BY ALL ORDER BY spend DESC LIMIT 10",
  "ELT": "SELECT customer_id, full_name, loyalty_tier, email_masked, phone_masked, total_spend FROM mart_top_customers"},
 "Q6 Rows rejected today, by file and by reason (latest run)": {
  "ETL": "SELECT source_file, reason, COUNT(*) n FROM (SELECT source_file, UNNEST(string_split(failure_reason,';')) reason FROM etl_quarantine WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id LIKE '%etl%' AND status='SUCCESS' ORDER BY start_time DESC LIMIT 1)) GROUP BY ALL ORDER BY 1, 3 DESC",
  "ELT": "SELECT source_file, reason, rows_rejected FROM mart_rejections WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id LIKE '%elt%' AND status='SUCCESS' ORDER BY start_time DESC LIMIT 1) ORDER BY 1, 3 DESC"},
}
for q, sqls in Q.items():
    print(f"\n## {q}")
    for side, sql in sqls.items():
        df = con.execute(sql).fetchdf()
        print(f"\n**{side}** — `{sql}`\n")
        print(df.to_markdown(index=False))
