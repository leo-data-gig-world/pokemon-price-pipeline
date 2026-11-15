"""Run monitoring: every stage records what it did, failed stages included.

  lake/_health/runs/<date>.json   one file per day: stages, timings, status
  lake/_health/history.json       every day rolled up for the monitoring charts

A day with no file at all is a run that never happened. The site shows it as
"no run", which is how a skipped schedule becomes visible.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import time
from contextlib import contextmanager

import pyarrow.compute as pc
import pyarrow.parquet as pq

from pipeline.extract import LAKE

STAGES = ["extract", "checks", "dbt", "summary", "spikes"]
WATCHLIST_SIZE = 5


def _now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


def _run_path(ds: str):
    return LAKE / "_health" / "runs" / f"{ds}.json"


def _load(ds: str) -> dict:
    p = _run_path(ds)
    return json.loads(p.read_text()) if p.exists() else {"date": ds, "stages": {}}


def _save(ds: str, data: dict) -> None:
    p = _run_path(ds)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2))


def start_run(ds: str) -> None:
    """Fresh record for this date, so a re-run never mixes with an old attempt."""
    server, repo, run_id = (os.environ.get(k, "") for k in ("GITHUB_SERVER_URL", "GITHUB_REPOSITORY",
                                                            "GITHUB_RUN_ID"))
    _save(ds, {"date": ds, "started_at": _now(), "stages": {},
               "trigger": os.environ.get("GITHUB_EVENT_NAME", "local"),
               "run_url": f"{server}/{repo}/actions/runs/{run_id}" if run_id else None})


def record(ds: str, stage: str, **values) -> None:
    data = _load(ds)
    data.setdefault("stages", {})[stage] = {**values, "recorded_at": _now()}
    _save(ds, data)


@contextmanager
def timed(ds: str, stage: str):
    """Time a stage and record it even when it fails. Yields a dict to add values to."""
    info: dict = {}
    t0 = time.perf_counter()
    try:
        yield info
        info["ok"] = True
    except Exception as e:
        info["ok"] = False
        info["error"] = str(e)[:300]
        raise
    finally:
        info["seconds"] = round(time.perf_counter() - t0, 1)
        record(ds, stage, **info)


def finish_run(ds: str) -> dict:
    """Decide the day's status, rebuild history.json, and fail loudly if the run failed."""
    data = _load(ds)
    stages = data.get("stages", {})
    failed = [s for s in STAGES if s in stages and not stages[s].get("ok")]
    missing = [s for s in STAGES if s not in stages]
    data["status"] = "passed" if not failed and not missing else "failed"
    data["failed_stage"] = (failed + missing + [None])[0]
    data["finished_at"] = _now()
    _save(ds, data)
    build_history()
    if data["status"] != "passed":
        raise RuntimeError(f"run for {ds} failed at stage {data['failed_stage']}")
    return data


def _watchlist() -> dict:
    """Price history for today's most expensive TCGplayer cards."""
    parts = sorted((LAKE / "raw" / "prices").glob("date=*"))
    if not parts:
        return {}
    cols = ["price_date", "card_id", "card_name", "set_id", "source", "variant", "market"]

    def tcg(path):
        t = pq.read_table(path / "prices.parquet", columns=cols)
        return t.filter(pc.equal(t["source"], "tcgplayer")).to_pylist()

    latest = [r for r in tcg(parts[-1]) if r["market"]]
    top = sorted(latest, key=lambda r: r["market"], reverse=True)[:WATCHLIST_SIZE]
    keys = {(r["card_id"], r["variant"]): r for r in top}
    series = {k: [] for k in keys}
    for part in parts:
        for r in tcg(part):
            k = (r["card_id"], r["variant"])
            if k in series and r["market"]:
                series[k].append({"date": r["price_date"].isoformat(), "usd": round(r["market"], 2)})
    return {"cards": [{"card": keys[k]["card_name"], "set": keys[k]["set_id"], "variant": k[1],
                       "points": series[k]} for k in keys]}


def _row(ds: str) -> dict:
    """One flat row per day for the charts. Older days without a run file fall back
    to the day's quality report, with stage timings unknown."""
    run = _load(ds) if _run_path(ds).exists() else {}
    health_path = LAKE / "_health" / f"{ds}.json"
    health = json.loads(health_path.read_text()) if health_path.exists() else {}
    st = run.get("stages", {})
    checks = health.get("checks", {})
    dbt = st.get("dbt", {})
    status = run.get("status") or ("passed" if health.get("passed") else "failed" if health else None)
    return {
        "date": ds, "status": status, "failed_stage": run.get("failed_stage"),
        "trigger": run.get("trigger"), "run_url": run.get("run_url"),
        "rows": health.get("rows"), "priced_share": health.get("priced_share"),
        "source_age_hours": health.get("source_age_hours"),
        "checks_passed": sum(bool(v) for v in checks.values()) if checks else None,
        "checks_total": len(checks) or None,
        "tests_pass": dbt.get("tests_pass"), "tests_fail": dbt.get("tests_fail"),
        "models_ok": dbt.get("models_ok"),
        "seconds": {s: st[s].get("seconds") for s in STAGES if s in st} or None,
        "spikes": st.get("spikes", {}).get("spikes"),
    }


def build_history() -> dict:
    dates = {p.stem for p in (LAKE / "_health" / "runs").glob("*.json")}
    dates |= {p.stem for p in (LAKE / "_health").glob("20??-??-??.json")}
    out = {"generated_at": _now(), "days": [_row(d) for d in sorted(dates)], "watchlist": _watchlist()}
    (LAKE / "_health" / "history.json").write_text(json.dumps(out, indent=2))
    return out
