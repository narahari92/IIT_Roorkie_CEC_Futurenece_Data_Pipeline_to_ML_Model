"""
Run audit + alerting for both NammaMart DAGs (Part F).

pipeline_runs: one row per DAG run — run_id, dag_id, start_time, end_time,
rows_extracted, rows_loaded, rows_quarantined, status. Written by the LAST task
with trigger_rule="all_done", so the row exists even when the run failed.
"""
from __future__ import annotations

import json
import urllib.request

import pendulum

from . import config as C
from . import warehouse as W


def _sum(values):
    return int(sum(v for v in values if v is not None))


METRICS_DDL = """CREATE TABLE IF NOT EXISTS pipeline_run_metrics (
    run_id VARCHAR, dag_id VARCHAR, metric VARCHAR, value DOUBLE, recorded_at TIMESTAMP)"""
VOLUME_DROP_ALERT_PCT = C.VOLUME_DROP_WARN_PCT      # 90
DISTRIBUTION_SHIFT_ALERT_PCT = 50                   # avg order value moved by more than ±50 %


def run_over_run_checks(con, dag_id: str, run_id: str, rows_extracted: int, batch_metrics_sql: str | None) -> list[str]:
    """Compare this run with the previous SUCCESS run of the same DAG (volume + distribution).
    Writes pipeline_run_metrics; returns a list of human-readable anomalies (empty = fine)."""
    con.execute(METRICS_DDL)
    metrics = {"rows_extracted": float(rows_extracted or 0)}
    if batch_metrics_sql:
        for name, val in zip(["batch_rows", "batch_avg_order_amount", "batch_net_revenue"],
                             con.execute(batch_metrics_sql, [run_id]).fetchone()):
            metrics[name] = float(val) if val is not None else None
    prev = dict(con.execute("""
        SELECT m.metric, m.value FROM pipeline_run_metrics m
        JOIN pipeline_runs r USING (run_id, dag_id)
        WHERE m.dag_id = ? AND r.status = 'SUCCESS' AND m.run_id <> ?
        QUALIFY row_number() OVER (PARTITION BY m.metric ORDER BY r.start_time DESC) = 1""", [dag_id, run_id]).fetchall())
    con.execute("DELETE FROM pipeline_run_metrics WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
    now = pendulum.now(C.TZ).naive()
    for k, v in metrics.items():
        con.execute("INSERT INTO pipeline_run_metrics VALUES (?,?,?,?,?)", [run_id, dag_id, k, v, now])

    anomalies = []
    def pct(cur, ref):
        return None if not ref else 100.0 * (cur - ref) / ref
    for k in ("rows_extracted", "batch_rows"):
        if k in metrics and k in prev and metrics[k] is not None:
            p = pct(metrics[k], prev[k])
            if p is not None and p <= -VOLUME_DROP_ALERT_PCT:
                anomalies.append(f"VOLUME: {k} {prev[k]:.0f} → {metrics[k]:.0f} ({p:+.0f}%)")
    k = "batch_avg_order_amount"
    if metrics.get(k) is not None and prev.get(k):
        p = pct(metrics[k], prev[k])
        if abs(p) >= DISTRIBUTION_SHIFT_ALERT_PCT:
            anomalies.append(f"DISTRIBUTION: avg order amount {prev[k]:.2f} → {metrics[k]:.2f} ({p:+.0f}%)")
    print("[Audit] run-over-run metrics: " + ", ".join(f"{k}={v:.2f}" if v is not None else f"{k}=None" for k, v in metrics.items())
          + (f" | previous: " + ", ".join(f"{k}={v:.2f}" for k, v in prev.items()) if prev else " | no previous SUCCESS run"))
    return anomalies


def write_run_audit(ti, dag_id: str, run_id: str, all_task_ids: list[str],
                    extracted: list, loaded, quarantined: list, start_time,
                    batch_metrics_sql: str | None = None) -> dict:
    """Collect sibling task states + XCom counts and upsert the pipeline_runs row,
    then run volume/distribution checks against the previous successful run."""
    states = ti.get_task_states(dag_id=dag_id, run_ids=[run_id], task_ids=all_task_ids).get(run_id, {})
    states = {k: v for k, v in states.items() if k != ti.task_id}
    failed = sorted(t for t, s in states.items() if s in ("failed", "upstream_failed"))
    skipped = sorted(t for t, s in states.items() if s == "skipped")
    not_done = sorted(t for t, s in states.items() if s not in ("success", "failed", "upstream_failed", "skipped"))
    status = "FAILED" if failed else ("SUCCESS" if not not_done else "PARTIAL")

    row = {
        "run_id": run_id, "dag_id": dag_id,
        "start_time": pendulum.instance(start_time).in_tz(C.TZ).naive() if start_time else None,
        "end_time": pendulum.now(C.TZ).naive(),
        "rows_extracted": _sum(extracted), "rows_loaded": _sum([loaded]), "rows_quarantined": _sum(quarantined),
        "status": status,
    }
    with W.connect() as con:
        W.write_pipeline_run(con, row)
        anomalies = run_over_run_checks(con, dag_id, run_id, row["rows_extracted"], batch_metrics_sql) if status != "FAILED" else []
        history = con.execute("""SELECT run_id, status, rows_extracted, rows_loaded, rows_quarantined,
                                        round(epoch(end_time - start_time)) AS seconds
                                 FROM pipeline_runs WHERE dag_id = ? ORDER BY start_time DESC LIMIT 5""", [dag_id]).fetchdf()
    print(f"[Audit] {dag_id} {run_id}: status={status} extracted={row['rows_extracted']} "
          f"loaded={row['rows_loaded']} quarantined={row['rows_quarantined']} "
          f"duration={(row['end_time'] - row['start_time']).total_seconds() if row['start_time'] else '?'}s")
    if failed:
        print(f"[Audit] failed/upstream_failed tasks: {failed}")
    if skipped:
        print(f"[Audit] skipped tasks: {skipped}")
    print("[Audit] last 5 runs:\n" + history.to_string(index=False))
    row["failed_tasks"] = failed
    row["anomalies"] = anomalies
    if anomalies:
        print("[Audit] ANOMALIES vs previous run: " + "; ".join(anomalies))
        alert(f":large_orange_diamond: *{dag_id}* run `{run_id}` succeeded but looks ANOMALOUS\n" + "\n".join(f"• {a}" for a in anomalies)
              + "\nCheck the source file before the 9:30 review.")
    if failed:
        alert(f":red_circle: *{dag_id}* run `{run_id}` FAILED\n"
              f"Failed tasks: {', '.join(failed)}\n"
              f"Rows extracted={row['rows_extracted']} loaded={row['rows_loaded']} quarantined={row['rows_quarantined']}\n"
              f"Open: Airflow → {dag_id} → Grid → {run_id}")
    return {k: (str(v) if hasattr(v, "isoformat") else v) for k, v in row.items()}


def alert(message: str) -> bool:
    """Slack incoming-webhook alert. The URL is an Airflow Variable (never in code).
    Without the Variable the message is printed, so the pipeline never fails because of alerting."""
    url = None
    try:
        from airflow.sdk import Variable
        url = Variable.get("SLACK_WEBHOOK_URL", default=None)
    except Exception as e:  # Variable API unavailable (e.g. local run)
        print(f"[Alert] Variable lookup unavailable ({e.__class__.__name__}); printing instead")
    if not url:
        print("[Alert] SLACK_WEBHOOK_URL not set — alert text:\n" + message)
        return False
    try:
        req = urllib.request.Request(url, data=json.dumps({"text": message}).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"[Alert] Slack responded {resp.status}")
            return True
    except Exception as e:
        print(f"[Alert] Slack post failed: {e}")
        return False


def on_task_failure(context):
    """DAG-level on_failure_callback: fires per failed task (after retries are exhausted)."""
    ti = context["task_instance"]
    exc = context.get("exception")
    alert(f":warning: *{ti.dag_id}* task `{ti.task_id}` failed (try {ti.try_number}) in run `{context['run_id']}`\n"
          f"Error: {str(exc)[:300]}")
