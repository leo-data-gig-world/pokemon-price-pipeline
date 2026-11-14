"""Data quality gate for one day's raw partition.

Fails loudly (raises) so Airflow retries and then alerts. Every run also writes a
health report to lake/_health/, which the portfolio page will read later.
"""
from __future__ import annotations

import datetime as dt
import json

import pyarrow.compute as pc
import pyarrow.parquet as pq

from pipeline.extract import LAKE, partition_path

MIN_ROWS = 100
MAX_DROP = 0.30          # fail if rows fall >30% vs the previous day
MIN_PRICED = 0.90        # at least 90% of rows need a market or low price
MAX_AGE_HOURS = 48       # source prices must be this fresh


def _previous_count(price_date: dt.date) -> int | None:
    days = sorted(p.name for p in (LAKE / "raw" / "prices").glob("date=*")
                  if p.name < f"date={price_date.isoformat()}")
    return pq.read_metadata(LAKE / "raw" / "prices" / days[-1] / "prices.parquet").num_rows if days else None


def run_checks(ds: str) -> dict:
    price_date = dt.date.fromisoformat(ds)
    t = pq.read_table(partition_path(price_date))
    n = t.num_rows
    prev = _previous_count(price_date)

    priced = pc.sum(pc.or_(pc.is_valid(t["market"]), pc.is_valid(t["low"]))).as_py() / n if n else 0
    newest = max(t["source_updated_at"].to_pylist() or [""])
    age_h = None
    if newest:
        age_h = (dt.datetime.now(dt.UTC)
                 - dt.datetime.fromisoformat(newest)).total_seconds() / 3600

    results = {
        "row_count": n >= MIN_ROWS,
        "no_big_drop": prev is None or n >= prev * (1 - MAX_DROP),
        "keys_not_null": t["card_id"].null_count == 0 and t["source"].null_count == 0,
        "mostly_priced": priced >= MIN_PRICED,
        # ponytail: freshness only judged on today's run; backfills of old dates skip it
        "fresh": age_h is None or price_date < dt.date.today() or age_h <= MAX_AGE_HOURS,
    }
    report = {
        "date": ds, "rows": n, "previous_rows": prev, "priced_share": round(priced, 3),
        "source_age_hours": round(age_h, 1) if age_h is not None else None,
        "checks": results, "passed": all(results.values()),
        "checked_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
    }
    health = LAKE / "_health"
    health.mkdir(parents=True, exist_ok=True)
    for name in (f"{ds}.json", "latest.json"):
        (health / name).write_text(json.dumps(report, indent=2))

    if not report["passed"]:
        failed = [k for k, ok in results.items() if not ok]
        raise ValueError(f"data quality failed on {ds}: {failed} | {report}")
    print(f"checks passed: {report}")
    return report
