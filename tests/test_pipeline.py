import datetime as dt
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

FIX = Path(__file__).parent / "fixtures"
DAY = dt.date(2026, 11, 14)


def card(name):
    return json.loads((FIX / f"card_{name}.json").read_text(encoding="utf-8"))


def test_normalize_charizard_has_both_sources():
    from pipeline.extract import normalize
    rows = normalize(card("charizard"), DAY)
    sources = {(r["source"], r["variant"]) for r in rows}
    assert ("cardmarket", "normal") in sources
    assert ("tcgplayer", "holofoil") in sources
    # cardmarket has no holo prices for this card, so no empty holo row
    assert ("cardmarket", "holo") not in sources
    assert all(r["card_id"] == "sv03.5-006" and r["price_date"] == DAY for r in rows)


def test_normalize_drops_zero_and_missing_prices():
    from pipeline.extract import normalize
    rows = normalize({"id": "x-1", "set": {"id": "x"}, "pricing": {
        "cardmarket": {"unit": "EUR", "avg": 0, "low": None, "trend": 0}}}, DAY)
    assert rows == []


def test_normalize_card_without_pricing():
    from pipeline.extract import normalize
    assert normalize({"id": "x-1"}, DAY) == []


def _write_day(lake, day, rows):
    from pipeline.extract import SCHEMA
    p = lake / "raw" / "prices" / f"date={day}" / "prices.parquet"
    p.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), p)


def _rows(n, day, priced=True):
    return [{"price_date": dt.date.fromisoformat(day), "card_id": f"c{i}", "set_id": "s",
             "card_name": "n", "rarity": "r", "source": "cardmarket", "variant": "normal",
             "currency": "EUR", "low": 1.0 if priced else None, "mid": None, "high": None,
             "market": 1.0 if priced else None, "trend": None, "avg7": None, "avg30": None,
             "source_updated_at": "2026-11-01T03:00:00Z"} for i in range(n)]


@pytest.fixture
def lake(tmp_path, monkeypatch):
    from pipeline import checks, extract
    monkeypatch.setattr(extract, "LAKE", tmp_path)
    monkeypatch.setattr(checks, "LAKE", tmp_path)
    from pipeline import summary
    monkeypatch.setattr(summary, "LAKE", tmp_path)
    return tmp_path


def test_checks_pass_and_write_health(lake):
    from pipeline.checks import run_checks
    _write_day(lake, "2026-11-01", _rows(200, "2026-11-01"))
    report = run_checks("2026-11-01")
    assert report["passed"]
    assert json.loads((lake / "_health" / "latest.json").read_text())["rows"] == 200


def test_checks_fail_on_big_drop(lake):
    from pipeline.checks import run_checks
    _write_day(lake, "2026-11-01", _rows(200, "2026-11-01"))
    _write_day(lake, "2026-11-02", _rows(120, "2026-11-02"))
    with pytest.raises(ValueError, match="no_big_drop"):
        run_checks("2026-11-02")


def test_checks_fail_when_prices_missing(lake):
    from pipeline.checks import run_checks
    _write_day(lake, "2026-11-01", _rows(200, "2026-11-01", priced=False))
    with pytest.raises(ValueError, match="mostly_priced"):
        run_checks("2026-11-01")


def test_dag_loads():
    pytest.importorskip("airflow")
    from airflow.models import DagBag
    bag = DagBag(dag_folder=str(Path(__file__).parents[1] / "dags"))
    assert bag.import_errors == {}
    d = bag.dags["pokemon_prices"]  # in-memory; get_dag() needs a database in Airflow 3
    assert [t.task_id for t in d.tasks] == ["extract_prices", "check_quality", "publish_summary"]
    assert d.default_args["retries"] == 2


def test_summary_top_cards_and_movers(lake):
    from pipeline.summary import build
    def rows(day, price):
        r = _rows(3, day)
        for i, x in enumerate(r):
            x.update(source="tcgplayer", variant="holofoil", market=price * (i + 1), card_name=f"card{i}")
        return r
    _write_day(lake, "2026-11-01", rows("2026-11-01", 10.0))
    _write_day(lake, "2026-11-02", rows("2026-11-02", 12.0))
    (lake / "_health").mkdir()
    out = build("2026-11-02")
    assert out["top_cards"][0]["price_usd"] == 36.0
    assert out["compared_to"] == "2026-11-01"
    assert out["movers"][0]["change_pct"] == 20.0
