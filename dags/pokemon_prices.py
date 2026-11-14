"""Daily Pokemon card price pipeline.

extract_prices -> check_quality -> publish_summary
  * runs daily at 06:30 UTC (prices refresh around 03:00 UTC)
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
        from pipeline import extract
        ds = get_current_context()["ds"]
        return str(extract.run(ds))

    @task
    def check_quality(_partition: str) -> dict:
        from pipeline import checks
        ds = get_current_context()["ds"]
        return checks.run_checks(ds)

    @task
    def publish_summary(_report: dict) -> dict:
        from pipeline import summary
        ds = get_current_context()["ds"]
        return summary.build(ds)

    publish_summary(check_quality(extract_prices()))


pokemon_prices()
