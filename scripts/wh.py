#!/usr/bin/env python
"""Query the NammaMart warehouse from the host.
    .venv/bin/python scripts/wh.py                       # list tables + row counts
    .venv/bin/python scripts/wh.py "select * from dq_results limit 5"
"""
import sys, duckdb, pandas as pd
pd.set_option("display.width", 220); pd.set_option("display.max_columns", 40); pd.set_option("display.max_rows", 200)
con = duckdb.connect("include/warehouse_hosur_narahari.duckdb", read_only=True)
if len(sys.argv) < 2:
    for (t,) in con.execute("select table_name from information_schema.tables order by 1").fetchall():
        print(f"{t:32} {con.execute(f'select count(*) from {t}').fetchone()[0]:>8} rows")
else:
    print(con.execute(" ".join(sys.argv[1:])).fetchdf().to_string(index=False))
