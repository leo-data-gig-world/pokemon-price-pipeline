"""Send a short alert to Discord if DISCORD_WEBHOOK_URL is set; otherwise just log.

GitHub also emails the repo owner when the scheduled workflow fails, so there is
always at least one alert path.
"""
import os

import requests


def send(text: str) -> None:
    url = os.environ.get("DISCORD_WEBHOOK_URL")
    print(f"ALERT: {text}")
    if url:
        requests.post(url, json={"content": text[:1900]}, timeout=15)


def on_failure(context) -> None:
    """Airflow on_failure_callback: fires after the last retry fails."""
    ti = context["task_instance"]
    send(f"pokemon_prices FAILED: task {ti.task_id} on {context.get('ds')}: "
         f"{context.get('exception')}")
