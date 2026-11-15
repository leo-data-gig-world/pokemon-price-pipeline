"""Run dbt (models + tests) as one Airflow task.

dbt lives in its own virtualenv (DBT_BIN) so its dependencies never fight
Airflow's pinned constraints. A failing dbt test fails the task, which retries
and then alerts like any other task. Test and model counts come from dbt's own
run_results.json, so the monitoring page shows exactly what dbt reported.
"""
from __future__ import annotations

import json
import os
import subprocess
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def summarize(run_results: Path) -> dict:
    if not run_results.exists():
        return {}
    results = json.loads(run_results.read_text())["results"]
    tests = Counter(r["status"] for r in results if r["unique_id"].startswith(("test.", "unit_test.")))
    models = Counter(r["status"] for r in results if r["unique_id"].startswith("model."))
    return {"tests_pass": tests.get("pass", 0),
            "tests_fail": tests.get("fail", 0) + tests.get("error", 0),
            "tests_warn": tests.get("warn", 0),
            "models_ok": models.get("success", 0),
            "models_error": models.get("error", 0)}


def dbt_build(stats: dict | None = None) -> dict:
    dbt = os.environ.get("DBT_BIN", "dbt")
    cmd = [dbt, "build", "--project-dir", "dbt", "--profiles-dir", "dbt"]
    print("running:", " ".join(cmd))
    proc = subprocess.run(cmd, check=False, cwd=ROOT)  # results are read before failing
    result = summarize(ROOT / "dbt" / "target" / "run_results.json")
    if stats is not None:
        stats.update(result)
    if proc.returncode:
        raise RuntimeError(f"dbt build failed: {result}")
    return result
