"""Publish a small daily summary for the portfolio site.

lake/_health/summary.json: the priciest cards today and the biggest movers vs the
previous day (TCGplayer market price, USD). Small on purpose: the site fetches it live.
"""
from __future__ import annotations

import datetime as dt
import json

import pyarrow.parquet as pq

from pipeline.extract import LAKE, partition_path

TOP_N = 8
MIN_PRICE = 2.0   # ignore movers under $2: tiny prices make huge, meaningless % swings


def _prices(path) -> dict[tuple, dict]:
    rows = pq.read_table(path, columns=["card_id", "card_name", "set_id", "source", "variant", "market"]).to_pylist()
    return {(r["card_id"], r["variant"]): r for r in rows if r["source"] == "tcgplayer" and r["market"]}


def _previous_partition(price_date: dt.date):
    days = sorted(p for p in (LAKE / "raw" / "prices").glob("date=*") if p.name < f"date={price_date}")
    return days[-1] / "prices.parquet" if days else None


def _card(r: dict) -> dict:
    return {"card": r["card_name"], "set": r["set_id"], "variant": r["variant"],
            "price_usd": round(r["market"], 2)}


def build(ds: str) -> dict:
    price_date = dt.date.fromisoformat(ds)
    today = _prices(partition_path(price_date))

    top = sorted(today.values(), key=lambda r: r["market"], reverse=True)[:TOP_N]
    movers = []
    prev_path = _previous_partition(price_date)
    if prev_path:
        prev = _prices(prev_path)
        for key, r in today.items():
            p = prev.get(key)
            if p and max(r["market"], p["market"]) >= MIN_PRICE:
                movers.append({**_card(r), "prev_usd": round(p["market"], 2),
                               "change_pct": round((r["market"] / p["market"] - 1) * 100, 1)})
        movers.sort(key=lambda m: abs(m["change_pct"]), reverse=True)

    out = {"date": ds, "cards_priced": len(today), "top_cards": [_card(r) for r in top],
           "movers": movers[:TOP_N], "compared_to": prev_path.parent.name[5:] if prev_path else None}
    (LAKE / "_health" / "summary.json").write_text(json.dumps(out, indent=2))
    print(f"summary: {len(out['top_cards'])} top cards, {len(out['movers'])} movers")
    return out
