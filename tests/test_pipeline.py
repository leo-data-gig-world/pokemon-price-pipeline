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
    from pipeline import spikes
    monkeypatch.setattr(spikes, "LAKE", tmp_path)
    from pipeline import metrics
    monkeypatch.setattr(metrics, "LAKE", tmp_path)
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
    assert sorted(t.task_id for t in d.tasks) == sorted(["extract_prices", "check_quality", "dbt_build", "publish_summary", "alert_spikes",
                    "record_run"])
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


def test_spike_alert_ignores_empty_export_and_other_days(lake, monkeypatch):
    from pipeline import spikes
    sent = []
    monkeypatch.setattr(spikes.alerts, "send", sent.append)
    (lake / "marts").mkdir()
    (lake / "_health").mkdir()
    rows = [{"price_date": None, "card_name": None},
            {"price_date": dt.date(2026, 11, 1), "card_name": "old"},
            {"price_date": dt.date(2026, 11, 2), "card_name": "Umbreon ex"}]
    tbl = pa.Table.from_pylist([{**r, "set_id": "s", "source": "tcgplayer", "variant": "holofoil",
                                 "market": 20.0, "currency": "USD", "avg_prev_7d": 10.0,
                                 "zscore_7d": 12.2} for r in rows])
    pq.write_table(tbl, lake / "marts" / "price_spikes.parquet")
    assert spikes.alert("2026-11-02") == 1
    assert "Umbreon ex" in sent[0]


def _all_stages_ok(ds):
    from pipeline import metrics
    metrics.start_run(ds)
    for st in metrics.STAGES:
        with metrics.timed(ds, st) as m:
            m["note"] = "ok"


def test_finish_run_passes_and_builds_history(lake):
    from pipeline import metrics
    _write_day(lake, "2026-11-01", _rows(200, "2026-11-01"))
    (lake / "_health").mkdir()
    _all_stages_ok("2026-11-01")
    assert metrics.finish_run("2026-11-01")["status"] == "passed"
    hist = json.loads((lake / "_health" / "history.json").read_text())
    assert [d["date"] for d in hist["days"]] == ["2026-11-01"]
    assert set(hist["days"][0]["seconds"]) == set(metrics.STAGES)


def test_failed_stage_is_recorded_and_raises(lake):
    from pipeline import metrics
    (lake / "_health").mkdir()
    metrics.start_run("2026-11-02")
    with pytest.raises(ValueError), metrics.timed("2026-11-02", "extract"):
        raise ValueError("API down")
    with pytest.raises(RuntimeError, match="extract"):
        metrics.finish_run("2026-11-02")
    day = json.loads((lake / "_health" / "history.json").read_text())["days"][0]
    assert day["status"] == "failed" and day["failed_stage"] == "extract"


def test_rerun_starts_fresh(lake):
    from pipeline import metrics
    (lake / "_health").mkdir()
    _all_stages_ok("2026-11-03")
    metrics.start_run("2026-11-03")                       # re-run: old stages must not linger
    with pytest.raises(RuntimeError, match="extract"):
        metrics.finish_run("2026-11-03")


def test_history_includes_days_from_before_monitoring(lake):
    from pipeline import metrics
    (lake / "_health").mkdir()
    (lake / "_health" / "2026-10-31.json").write_text(json.dumps(
        {"date": "2026-10-31", "rows": 150, "passed": True, "checks": {"a": True, "b": True}}))
    day = metrics.build_history()["days"][0]
    assert day["status"] == "passed" and day["rows"] == 150 and day["seconds"] is None


def test_watchlist_tracks_top_cards_over_days(lake):
    from pipeline import metrics
    for d, price in [("2026-11-01", 10.0), ("2026-11-02", 12.0)]:
        r = _rows(3, d)
        for i, x in enumerate(r):
            x.update(source="tcgplayer", variant="holofoil", market=price * (i + 1), card_name=f"c{i}")
        _write_day(lake, d, r)
    (lake / "_health").mkdir()
    wl = metrics.build_history()["watchlist"]["cards"]
    assert wl[0]["card"] == "c2" and [p["usd"] for p in wl[0]["points"]] == [30.0, 36.0]


def test_dbt_summary_counts(tmp_path):
    from pipeline.transform import summarize
    p = tmp_path / "run_results.json"
    p.write_text(json.dumps({"results": [
        {"unique_id": "model.x.a", "status": "success"}, {"unique_id": "test.x.t1", "status": "pass"},
        {"unique_id": "test.x.t2", "status": "fail"}, {"unique_id": "unit_test.x.u", "status": "pass"}]}))
    assert summarize(p) == {"tests_pass": 2, "tests_fail": 1, "tests_warn": 0, "models_ok": 1, "models_error": 0}
