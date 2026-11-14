"""Alert on today's price spikes found by dbt, and publish them for the site."""
from __future__ import annotations

import datetime as dt
import json

import pyarrow.compute as pc
import pyarrow.parquet as pq

from pipeline import alerts
from pipeline.extract import LAKE


def alert(ds: str) -> int:
    path = LAKE / "marts" / "price_spikes.parquet"
    t = pq.read_table(path)
    # an empty dbt-duckdb export comes back as one all-null row; drop it
    t = t.filter(pc.is_valid(t["price_date"]))
    today = [r for r in t.to_pylist() if r["price_date"] == dt.date.fromisoformat(ds)]

    (LAKE / "_health" / "spikes.json").write_text(json.dumps(
        {"date": ds, "spikes": [{**r, "price_date": r["price_date"].isoformat()} for r in today]},
        indent=2))
    if today:
        lines = [f"{r['card_name']} ({r['set_id']}, {r['source']} {r['variant']}): "
                 f"{r['market']} {r['currency']} vs 7-day avg {r['avg_prev_7d']} (z={r['zscore_7d']})"
                 for r in today[:10]]
        alerts.send(f"{len(today)} price spike(s) on {ds}:\n" + "\n".join(lines))
    print(f"{len(today)} spikes on {ds}")
    return len(today)
