#!/usr/bin/env python
"""Bonus: data-quality trend chart from dq_results, written as docs/dq_trend.svg (no extra packages).
One point per run of the ETL DAG: Critical rows failed, Warning rows failed, rows_quarantined share."""
import duckdb
con = duckdb.connect("include/warehouse_hosur_narahari.duckdb", read_only=True)
rows = con.execute("""
    SELECT d.run_id, min(d.checked_at) t,
           sum(rows_failed) FILTER (WHERE severity='Critical') crit,
           sum(rows_failed) FILTER (WHERE severity='Warning')  warn,
           max(rows_checked) FILTER (WHERE check_name LIKE 'orders.%') orders_in,
           any_value(r.status) status
    FROM dq_results d LEFT JOIN pipeline_runs r USING (run_id, dag_id)
    WHERE d.dag_id = 'nammamart_etl_hosur_narahari' AND d.check_name NOT LIKE 'orders_increment.%'
    GROUP BY 1 ORDER BY 2""").fetchall()
W, H, L, B = 980, 360, 60, 60
n = len(rows); ymax = max(max(r[2] or 0, r[3] or 0) for r in rows) * 1.15 or 1
x = lambda i: L + i * (W - L - 20) / max(n - 1, 1)
y = lambda v: H - B - (v or 0) / ymax * (H - B - 30)
def path(idx): return "M " + " L ".join(f"{x(i):.1f},{y(r[idx]):.1f}" for i, r in enumerate(rows))
svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" font-family="Helvetica, Arial" font-size="11">',
       f'<rect width="{W}" height="{H}" fill="white"/>',
       f'<text x="{L}" y="18" font-size="14" font-weight="bold">NammaMart data-quality trend — rows failing per run (ETL DAG, from dq_results)</text>',
       f'<line x1="{L}" y1="{H-B}" x2="{W-20}" y2="{H-B}" stroke="#999"/>', f'<line x1="{L}" y1="30" x2="{L}" y2="{H-B}" stroke="#999"/>']
for k in range(5):
    v = ymax * k / 4; svg.append(f'<text x="{L-6}" y="{y(v)+4:.1f}" text-anchor="end" fill="#666">{v:.0f}</text>')
    svg.append(f'<line x1="{L}" y1="{y(v):.1f}" x2="{W-20}" y2="{y(v):.1f}" stroke="#eee"/>')
svg.append(f'<path d="{path(2)}" fill="none" stroke="#d62728" stroke-width="2"/>')
svg.append(f'<path d="{path(3)}" fill="none" stroke="#ff9900" stroke-width="2"/>')
for i, r in enumerate(rows):
    svg.append(f'<circle cx="{x(i):.1f}" cy="{y(r[2]):.1f}" r="3.5" fill="#d62728"><title>{r[0]}\nCritical rows failed: {r[2]}</title></circle>')
    svg.append(f'<circle cx="{x(i):.1f}" cy="{y(r[3]):.1f}" r="3.5" fill="#ff9900"><title>{r[0]}\nWarning rows failed: {r[3]}</title></circle>')
    lbl = r[1].strftime("%d %H:%M"); svg.append(f'<text x="{x(i):.1f}" y="{H-B+14}" text-anchor="middle" fill="#666" font-size="9">{lbl}</text>')
    svg.append(f'<text x="{x(i):.1f}" y="{H-B+26}" text-anchor="middle" fill="#999" font-size="8">{r[4]} in</text>')
svg.append(f'<rect x="{W-230}" y="34" width="12" height="12" fill="#d62728"/><text x="{W-212}" y="44">Critical rows failed (quarantined)</text>')
svg.append(f'<rect x="{W-230}" y="52" width="12" height="12" fill="#ff9900"/><text x="{W-212}" y="62">Warning rows failed (loaded, logged)</text>')
svg.append('</svg>')
open("docs/dq_trend.svg", "w").write("\n".join(svg))
print(f"docs/dq_trend.svg written: {n} runs")
for r in rows: print(f"  {r[1]:%m-%d %H:%M}  crit={int(r[2] or 0):>4} warn={int(r[3] or 0):>4} orders_in={r[4]} {r[5]}")
