"""Run dbt (models + tests) as one Airflow task.

dbt lives in its own virtualenv (DBT_BIN) so its dependencies never fight
Airflow's pinned constraints. A failing dbt test fails the task, which retries
and then alerts like any other task.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def dbt_build() -> None:
    dbt = os.environ.get("DBT_BIN", "dbt")
    cmd = [dbt, "build", "--project-dir", "dbt", "--profiles-dir", "dbt"]
    print("running:", " ".join(cmd))
    subprocess.run(cmd, cwd=ROOT, check=True)
