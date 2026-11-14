"""Pull today's Pokemon card prices from the free TCGdex API into the lake.

One row per card x price source x variant, written as one Parquet file per day:
    lake/raw/prices/date=YYYY-MM-DD/prices.parquet
Re-running a day overwrites that day's file, so loads are idempotent.
"""
from __future__ import annotations

import datetime as dt
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

API = "https://api.tcgdex.net/v2/en"
# ponytail: three popular sets (~630 cards); add set ids here to widen coverage.
SETS = ["sv03.5", "sv08.5", "sv10"]
HEADERS = {"User-Agent": "pokemon-price-pipeline (portfolio project)"}
WORKERS = 4  # polite: free API, keep concurrency low

LAKE = Path(os.environ.get("LAKE_DIR", Path(__file__).resolve().parents[1] / "lake"))

SCHEMA = pa.schema([
    ("price_date", pa.date32()),
    ("card_id", pa.string()),
    ("set_id", pa.string()),
    ("card_name", pa.string()),
    ("rarity", pa.string()),
    ("source", pa.string()),       # cardmarket | tcgplayer
    ("variant", pa.string()),      # normal | holo | holofoil | reverseHolofoil ...
    ("currency", pa.string()),
    ("low", pa.float64()),
    ("mid", pa.float64()),
    ("high", pa.float64()),
    ("market", pa.float64()),
    ("trend", pa.float64()),
    ("avg7", pa.float64()),
    ("avg30", pa.float64()),
    ("source_updated_at", pa.string()),
])


def _num(v):
    return float(v) if isinstance(v, (int, float)) and v > 0 else None


def normalize(card: dict, price_date: dt.date) -> list[dict]:
    """Flatten one TCGdex card into price rows. Cards with no prices give []."""
    base = {
        "price_date": price_date,
        "card_id": card["id"],
        "set_id": card.get("set", {}).get("id"),
        "card_name": card.get("name"),
        "rarity": card.get("rarity"),
    }
    rows = []
    pricing = card.get("pricing") or {}

    cm = pricing.get("cardmarket")
    if cm:
        for variant, sfx in (("normal", ""), ("holo", "-holo")):
            row = {
                **base, "source": "cardmarket", "variant": variant, "currency": cm.get("unit"),
                "low": _num(cm.get("low" + sfx)), "mid": None, "high": None,
                "market": _num(cm.get("avg" + sfx)), "trend": _num(cm.get("trend" + sfx)),
                "avg7": _num(cm.get("avg7" + sfx)), "avg30": _num(cm.get("avg30" + sfx)),
                "source_updated_at": cm.get("updated"),
            }
            if row["market"] or row["low"] or row["trend"]:
                rows.append(row)

    tp = pricing.get("tcgplayer")
    if tp:
        for variant, p in tp.items():
            if not isinstance(p, dict):
                continue  # skips "unit" and "updated"
            row = {
                **base, "source": "tcgplayer", "variant": variant, "currency": tp.get("unit"),
                "low": _num(p.get("lowPrice")), "mid": _num(p.get("midPrice")),
                "high": _num(p.get("highPrice")), "market": _num(p.get("marketPrice")),
                "trend": None, "avg7": None, "avg30": None,
                "source_updated_at": tp.get("updated"),
            }
            if row["market"] or row["low"]:
                rows.append(row)
    return rows


def _get(session: requests.Session, url: str) -> dict:
    r = session.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_cards(set_ids=SETS, limit: int | None = None) -> list[dict]:
    with requests.Session() as s:
        ids = [c["id"] for sid in set_ids for c in _get(s, f"{API}/sets/{sid}")["cards"]]
        ids = ids[:limit] if limit else ids
        with ThreadPoolExecutor(WORKERS) as pool:
            return list(pool.map(lambda cid: _get(s, f"{API}/cards/{cid}"), ids))


def partition_path(price_date: dt.date) -> Path:
    return LAKE / "raw" / "prices" / f"date={price_date.isoformat()}" / "prices.parquet"


def run(ds: str, limit: int | None = None) -> Path:
    """Extract one day and overwrite its partition. `ds` is Airflow's run date."""
    price_date = dt.date.fromisoformat(ds)
    rows = [r for card in fetch_cards(limit=limit) for r in normalize(card, price_date)]
    if not rows:
        raise RuntimeError("API returned no prices")
    out = partition_path(price_date)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), tmp)
    tmp.replace(out)  # atomic swap: a crash never leaves half a file
    print(f"wrote {len(rows)} rows -> {out}")
    return out


if __name__ == "__main__":
    import sys
    run(sys.argv[1] if len(sys.argv) > 1 else dt.date.today().isoformat(),
        limit=int(sys.argv[2]) if len(sys.argv) > 2 else None)
