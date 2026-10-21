
## Q1 Daily net revenue by zone and store (delivered only, after discount) — shown as September totals

**ETL** — `SELECT zone, store_id, store_name, SUM(delivered_orders) delivered, ROUND(SUM(net_revenue),2) net_revenue FROM etl_daily_zone_revenue WHERE order_date BETWEEN '2026-09-01' AND '2026-09-30' GROUP BY ALL ORDER BY zone, net_revenue DESC`

| zone    | store_id   | store_name                  |   delivered |   net_revenue |
|:--------|:-----------|:----------------------------|------------:|--------------:|
| East    | S10        | NammaMart Marathahalli      |         137 |       78968   |
| East    | S11        | NammaMart HSR Layout        |         143 |       75245.4 |
| East    | S08        | NammaMart Whitefield        |         149 |       72410.6 |
| East    | S09        | NammaMart Indiranagar       |         128 |       63660.2 |
| North   | S03        | NammaMart RT Nagar          |         137 |       76722.1 |
| North   | S01        | NammaMart Hebbal            |         136 |       76552.4 |
| North   | S02        | NammaMart Yelahanka         |         139 |       72366.7 |
| South   | S06        | NammaMart Banashankari      |         147 |       89363.3 |
| South   | S05        | NammaMart JP Nagar          |         129 |       77378.4 |
| South   | S04        | NammaMart Jayanagar         |         133 |       73626.1 |
| Unknown | S07        | NammaMart BTM Layout        |         145 |       82725.6 |
| West    | S13        | NammaMart Malleshwaram      |         141 |       85151.6 |
| West    | S15        | NammaMart Basaveshwaranagar |         150 |       79323.9 |
| West    | S14        | NammaMart Vijayanagar       |         151 |       70951.9 |
| West    | S12        | NammaMart Rajajinagar       |         113 |       56988.8 |

**ELT** — `SELECT zone, store_id, store_name, SUM(delivered_orders) delivered, ROUND(SUM(net_revenue),2) net_revenue FROM mart_daily_store_kpis WHERE order_date BETWEEN '2026-09-01' AND '2026-09-30' GROUP BY ALL ORDER BY zone, net_revenue DESC`

| zone    | store_id   | store_name                  |   delivered |   net_revenue |
|:--------|:-----------|:----------------------------|------------:|--------------:|
| East    | S10        | NammaMart Marathahalli      |         137 |       78968   |
| East    | S11        | NammaMart HSR Layout        |         143 |       75245.4 |
| East    | S08        | NammaMart Whitefield        |         149 |       72410.6 |
| East    | S09        | NammaMart Indiranagar       |         128 |       63660.2 |
| North   | S03        | NammaMart RT Nagar          |         137 |       76722.1 |
| North   | S01        | NammaMart Hebbal            |         136 |       76552.4 |
| North   | S02        | NammaMart Yelahanka         |         139 |       72366.7 |
| South   | S06        | NammaMart Banashankari      |         147 |       89363.3 |
| South   | S05        | NammaMart JP Nagar          |         129 |       77378.4 |
| South   | S04        | NammaMart Jayanagar         |         133 |       73626.1 |
| Unknown | S07        | NammaMart BTM Layout        |         145 |       82725.6 |
| West    | S13        | NammaMart Malleshwaram      |         141 |       85151.6 |
| West    | S15        | NammaMart Basaveshwaranagar |         150 |       79323.9 |
| West    | S14        | NammaMart Vijayanagar       |         151 |       70951.9 |
| West    | S12        | NammaMart Rajajinagar       |         113 |       56988.8 |

## Q2 Revenue and gross margin by category

**ETL** — `SELECT p.category, ROUND(SUM(f.net_revenue),2) revenue, ROUND(SUM(f.gross_margin),2) gross_margin, ROUND(100*SUM(f.gross_margin)/SUM(f.net_revenue),2) margin_pct FROM etl_fact_orders f JOIN etl_dim_product p USING (product_id) WHERE f.is_delivered AND f.order_date <= '2026-09-30' GROUP BY 1 ORDER BY 2 DESC`

| category            |   revenue |   gross_margin |   margin_pct |
|:--------------------|----------:|---------------:|-------------:|
| Staples             |  266501   |       58728.2  |        22.04 |
| Personal Care       |  204386   |       44300.7  |        21.67 |
| Household           |  199167   |       51002.5  |        25.61 |
| Dairy & Bakery      |  167298   |       43902.2  |        26.24 |
| Snacks & Beverages  |  161871   |       37722.1  |        23.3  |
| Fruits & Vegetables |   93031.7 |       19507.6  |        20.97 |
| Unknown             |   39180.7 |        4475.55 |        11.42 |

**ELT** — `SELECT p.category, ROUND(SUM(o.order_amount),2) revenue, ROUND(SUM(o.order_amount - o.quantity*p.cost_price),2) gross_margin, ROUND(100*SUM(o.order_amount - o.quantity*p.cost_price)/SUM(o.order_amount),2) margin_pct FROM stg_orders o JOIN stg_products p USING (product_id) WHERE o.dq_status='PASSED' AND o.order_status='DELIVERED' AND o.order_date <= '2026-09-30' GROUP BY 1 ORDER BY 2 DESC`

| category            |   revenue |   gross_margin |   margin_pct |
|:--------------------|----------:|---------------:|-------------:|
| Staples             |  266501   |       58728.2  |        22.04 |
| Personal Care       |  204386   |       44300.7  |        21.67 |
| Household           |  199167   |       51002.5  |        25.61 |
| Dairy & Bakery      |  167298   |       43902.2  |        26.24 |
| Snacks & Beverages  |  161871   |       37722.1  |        23.3  |
| Fruits & Vegetables |   93031.7 |       19507.6  |        20.97 |
| Unknown             |   39180.7 |        4475.55 |        11.42 |

## Q3 Average delivery time per store and % within 15 minutes

**ETL** — `SELECT s.store_id, s.store_name, ROUND(AVG(f.delivery_minutes),2) avg_minutes, ROUND(100.0*AVG(f.within_15_min::INT),2) pct_within_15 FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id) WHERE f.is_delivered AND f.order_date <= '2026-09-30' GROUP BY ALL ORDER BY 4 DESC`

| store_id   | store_name                  |   avg_minutes |   pct_within_15 |
|:-----------|:----------------------------|--------------:|----------------:|
| S08        | NammaMart Whitefield        |         20.55 |           36.24 |
| S07        | NammaMart BTM Layout        |         21.34 |           32.41 |
| S01        | NammaMart Hebbal            |         20.71 |           30.88 |
| S03        | NammaMart RT Nagar          |         20.67 |           29.93 |
| S14        | NammaMart Vijayanagar       |         21.96 |           29.8  |
| S05        | NammaMart JP Nagar          |         21.54 |           29.46 |
| S02        | NammaMart Yelahanka         |         21.32 |           28.78 |
| S11        | NammaMart HSR Layout        |         21.09 |           28.67 |
| S13        | NammaMart Malleshwaram      |         21.88 |           27.66 |
| S09        | NammaMart Indiranagar       |         21.2  |           27.34 |
| S04        | NammaMart Jayanagar         |         21.4  |           27.07 |
| S10        | NammaMart Marathahalli      |         22.12 |           26.28 |
| S12        | NammaMart Rajajinagar       |         22.21 |           22.12 |
| S15        | NammaMart Basaveshwaranagar |         22.31 |           22    |
| S06        | NammaMart Banashankari      |         22.53 |           21.77 |

**ELT** — `SELECT store_id, store_name, avg_delivery_minutes, pct_within_15 FROM mart_delivery_sla ORDER BY pct_within_15 DESC`

| store_id   | store_name                  |   avg_delivery_minutes |   pct_within_15 |
|:-----------|:----------------------------|-----------------------:|----------------:|
| S08        | NammaMart Whitefield        |                  20.61 |           35.37 |
| S07        | NammaMart BTM Layout        |                  21.13 |           32.45 |
| S01        | NammaMart Hebbal            |                  20.47 |           32.21 |
| S02        | NammaMart Yelahanka         |                  20.93 |           31.33 |
| S14        | NammaMart Vijayanagar       |                  21.51 |           31.33 |
| S05        | NammaMart JP Nagar          |                  21.19 |           29.58 |
| S03        | NammaMart RT Nagar          |                  20.63 |           29.17 |
| S11        | NammaMart HSR Layout        |                  20.82 |           28.57 |
| S04        | NammaMart Jayanagar         |                  21.31 |           27.54 |
| S13        | NammaMart Malleshwaram      |                  21.87 |           27.33 |
| S09        | NammaMart Indiranagar       |                  21.13 |           26.47 |
| S10        | NammaMart Marathahalli      |                  22.09 |           26    |
| S12        | NammaMart Rajajinagar       |                  21.83 |           24.37 |
| S15        | NammaMart Basaveshwaranagar |                  22.03 |           22.93 |
| S06        | NammaMart Banashankari      |                  22.52 |           21.57 |

## Q4 Revenue by payment mode and the total that does not reconcile

**ETL** — `WITH r AS (SELECT f.payment_mode, ROUND(SUM(ABS(x.delta)),2) unreconciled FROM etl_payment_reconciliation x JOIN etl_fact_orders f USING (order_id) WHERE x.reason IN ('delivered_without_payment','delivered_but_payment_failed','amount_mismatch') GROUP BY 1) SELECT f.payment_mode, ROUND(SUM(f.net_revenue),2) net_revenue, COALESCE(r.unreconciled,0) unreconciled FROM etl_fact_orders f LEFT JOIN r USING (payment_mode) WHERE f.is_delivered GROUP BY ALL ORDER BY 2 DESC`

| payment_mode     |   net_revenue |   unreconciled |
|:-----------------|--------------:|---------------:|
| UPI              |        639932 |        32419.4 |
| Card             |        251882 |        23615.8 |
| Cash on Delivery |        182334 |        26157   |
| Wallet           |        138118 |        14866.3 |

**ELT** — `SELECT payment_mode, net_revenue, unreconciled_amount, unreconciled_pct FROM mart_payment_mode_revenue`

| payment_mode     |   net_revenue |   unreconciled_amount |   unreconciled_pct |
|:-----------------|--------------:|----------------------:|-------------------:|
| UPI              |        639932 |               32419.4 |               5.07 |
| Card             |        251882 |               23615.8 |               9.38 |
| Cash on Delivery |        182334 |               26157   |              14.35 |
| Wallet           |        138118 |               14866.3 |              10.76 |

## Q5 Top 10 customers by spend with loyalty tier (no raw email/phone)

**ETL** — `SELECT c.customer_id, c.full_name, c.loyalty_tier, c.email_masked, c.phone_masked, ROUND(SUM(f.net_revenue),2) spend FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id) WHERE f.is_delivered GROUP BY ALL ORDER BY spend DESC LIMIT 10`

| customer_id   | full_name     | loyalty_tier   | email_masked      | phone_masked   |   spend |
|:--------------|:--------------|:---------------|:------------------|:---------------|--------:|
| C00136        | Sanjay Bhat   | Bronze         | sa***@example.com | 96******88     | 8738.1  |
| C00115        | Sanjay Kumar  | Bronze         | sa***@example.com | 96******85     | 8037.7  |
| C00283        | Nikhil Rao    | Bronze         | ni***@example.com | 91******43     | 7934.6  |
| C00236        | Sneha Naidu   | Platinum       | sn***@example.com | 90******01     | 7831.8  |
| C00310        | Kiran Patil   | Bronze         | ki***@example     | 91******35     | 7258.95 |
| C00265        | Vikram Hegde  | Gold           | vi***@example.com | 91******69     | 7151.3  |
| C00071        | Mahesh Pillai | Silver         | ma***@example.com | 91******42     | 6933.3  |
| C00338        | Bhavana Iyer  | Silver         | bh***@example.com | 94******05     | 6848.55 |
| C00357        | Arjun Naidu   | Bronze         | ar***@example.com | 93*****12      | 6637.9  |
| C00209        | Kavya Pillai  | Platinum       | ka***@example.com | 98******97     | 6519.4  |

**ELT** — `SELECT customer_id, full_name, loyalty_tier, email_masked, phone_masked, total_spend FROM mart_top_customers`

| customer_id   | full_name     | loyalty_tier   | email_masked      | phone_masked   |   total_spend |
|:--------------|:--------------|:---------------|:------------------|:---------------|--------------:|
| C00136        | Sanjay Bhat   | Bronze         | sa***@example.com | 96******88     |       8738.1  |
| C00115        | Sanjay Kumar  | Bronze         | sa***@example.com | 96******85     |       8037.7  |
| C00283        | Nikhil Rao    | Bronze         | ni***@example.com | 91******43     |       7934.6  |
| C00236        | Sneha Naidu   | Platinum       | sn***@example.com | 90******01     |       7831.8  |
| C00310        | Kiran Patil   | Bronze         | ki***@example     | 91******35     |       7258.95 |
| C00265        | Vikram Hegde  | Gold           | vi***@example.com | 91******69     |       7151.3  |
| C00071        | Mahesh Pillai | Silver         | ma***@example.com | 91******42     |       6933.3  |
| C00338        | Bhavana Iyer  | Silver         | bh***@example.com | 94******05     |       6848.55 |
| C00357        | Arjun Naidu   | Bronze         | ar***@example.com | 93*****12      |       6637.9  |
| C00209        | Kavya Pillai  | Platinum       | ka***@example.com | 98******97     |       6519.4  |

## Q6 Rows rejected today, by file and by reason (latest run)

**ETL** — `SELECT source_file, reason, COUNT(*) n FROM (SELECT source_file, UNNEST(string_split(failure_reason,';')) reason FROM etl_quarantine WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id LIKE '%etl%' AND status='SUCCESS' ORDER BY start_time DESC LIMIT 1)) GROUP BY ALL ORDER BY 1, 3 DESC`

| source_file   | reason                                |   n |
|:--------------|:--------------------------------------|----:|
| customers.csv | customers.customer_id_unique          |   5 |
| orders.csv    | orders.order_amount_matches_formula   |  18 |
| orders.csv    | orders.customer_id_not_null           |  18 |
| orders.csv    | orders.order_id_unique                |  12 |
| orders.csv    | orders.quantity_positive              |  10 |
| orders.csv    | orders.product_id_in_master           |   9 |
| orders.csv    | orders.delivered_has_delivery_minutes |   8 |
| orders.csv    | orders.delivery_minutes_below_120     |   6 |
| orders.csv    | orders.store_id_in_master             |   5 |
| orders.csv    | orders.order_ts_not_future            |   5 |
| orders.csv    | orders.order_ts_parses                |   4 |
| orders.csv    | orders.order_status_in_list           |   3 |
| orders.csv    | orders.discount_pct_0_100             |   3 |
| orders.csv    | orders.order_ts_not_null              |   2 |
| payments.csv  | payments.success_amount_positive      |  10 |
| payments.csv  | payments.order_id_in_orders           |   8 |
| payments.csv  | payments.payment_ts_not_future        |   5 |
| payments.csv  | payments.payment_id_unique            |   4 |
| products.csv  | products.product_id_unique            |   2 |
| products.csv  | products.superseded_old_row           |   2 |

**ELT** — `SELECT source_file, reason, rows_rejected FROM mart_rejections WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id LIKE '%elt%' AND status='SUCCESS' ORDER BY start_time DESC LIMIT 1) ORDER BY 1, 3 DESC`

| source_file   | reason                                |   rows_rejected |
|:--------------|:--------------------------------------|----------------:|
| customers.csv | customers.customer_id_unique          |               5 |
| orders.csv    | orders.customer_id_not_null           |              18 |
| orders.csv    | orders.order_amount_matches_formula   |              18 |
| orders.csv    | orders.order_id_unique                |              12 |
| orders.csv    | orders.quantity_positive              |              10 |
| orders.csv    | orders.product_id_in_master           |               9 |
| orders.csv    | orders.delivered_has_delivery_minutes |               8 |
| orders.csv    | orders.delivery_minutes_below_120     |               6 |
| orders.csv    | orders.order_ts_not_future            |               5 |
| orders.csv    | orders.store_id_in_master             |               5 |
| orders.csv    | orders.order_ts_parses                |               4 |
| orders.csv    | orders.discount_pct_0_100             |               3 |
| orders.csv    | orders.order_status_in_list           |               3 |
| orders.csv    | orders.order_ts_not_null              |               2 |
| payments.csv  | payments.success_amount_positive      |              10 |
| payments.csv  | payments.order_id_in_orders           |               8 |
| payments.csv  | payments.payment_ts_not_future        |               5 |
| payments.csv  | payments.payment_id_unique            |               4 |
| products.csv  | products.superseded_old_row           |               2 |
| products.csv  | products.product_id_unique            |               2 |
