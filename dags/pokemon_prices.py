"""Daily Pokemon card price pipeline.

extract_prices -> check_quality -> dbt_build -> [publish_summary, alert_spikes] -> record_run
  * runs daily at 06:30 UTC (prices refresh around 03:00 UTC)
  * every task records its timing and result; record_run runs even after a failure,
    so failed days show up on the monitoring page instead of disappearing
  * each task retries twice, 5 minutes apart, then alerts
  * idempotent: re-running a date overwrites only that date's partition,
    so backfills are safe: `airflow dags test pokemon_prices 2026-11-01`
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # repo root -> `pipeline`
# Pipeline code is imported inside tasks, not here: Airflow re-parses this file
# often, so top-level imports stay light (an Airflow best practice).


def _alert(context):
    from pipeline import alerts
    alerts.on_failure(context)


DEFAULT_ARGS = {
    "owner": "leo",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _alert,
}


@dag(
    dag_id="pokemon_prices",
    schedule="30 6 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,               # no surprise backfills; we backfill on purpose
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["portfolio", "prices"],
)
def pokemon_prices():
    @task
    def extract_prices() -> str:
        import pyarrow.parquet as pq

        from pipeline import extract, metrics
        ds = get_current_context()["ds"]
        metrics.start_run(ds)
        with metrics.timed(ds, "extract") as m:
            path = extract.run(ds)
            m["rows"] = pq.read_metadata(path).num_rows
        return str(path)

    @task
    def check_quality(_partition: str) -> dict:
        from pipeline import checks, metrics
        ds = get_current_context()["ds"]
        with metrics.timed(ds, "checks"):
            return checks.run_checks(ds)

    @task
    def dbt_build(_report: dict) -> None:
        from pipeline import metrics, transform
        ds = get_current_context()["ds"]
        with metrics.timed(ds, "dbt") as m:
            transform.dbt_build(m)

    @task
    def publish_summary() -> dict:
        from pipeline import metrics, summary
        ds = get_current_context()["ds"]
        with metrics.timed(ds, "summary"):
            return summary.build(ds)

    @task
    def alert_spikes() -> int:
        from pipeline import metrics, spikes
        ds = get_current_context()["ds"]
        with metrics.timed(ds, "spikes") as m:
            m["spikes"] = spikes.alert(ds)
            return m["spikes"]

    @task(trigger_rule="all_done", retries=0, on_failure_callback=None)
    def record_run() -> dict:
        from pipeline import metrics
        return metrics.finish_run(get_current_context()["ds"])

    built = dbt_build(check_quality(extract_prices()))
    built >> [publish_summary(), alert_spikes()] >> record_run()

pokemon_prices()
