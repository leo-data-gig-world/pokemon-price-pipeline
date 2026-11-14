# Pokémon card price pipeline

A small, real data pipeline: every morning it pulls Pokémon card prices from two
marketplaces, checks the data, and stores one partition per day in a Parquet data lake.

Designed around the parts of data engineering that matter in production:
**orchestration, idempotent loads, backfills, data quality gates, alerting and CI/CD**.

```
                      Airflow DAG  pokemon_prices  (daily 06:30 UTC)
 TCGdex API ──► extract_prices ──► check_quality ──► publish_summary ──► lake/_health/summary.json
 (Cardmarket EUR,        │                     │
  TCGplayer USD)         ▼                     └─ fails? retry x2, then alert
          lake/raw/prices/date=YYYY-MM-DD/prices.parquet
```

## How it runs (and why this way)

There is no always-on server. A scheduled GitHub Actions workflow starts a fresh
Linux machine, installs **Apache Airflow 3**, and runs the DAG once with
`airflow dags test`. Then it commits the new partition back to this repo.

Real Airflow, real DAG, zero hosting cost. What it skips is the always-on scheduler
and web UI, which a one-task-a-day pipeline doesn't need.

## What's in it

| Piece | Where | What it shows |
|---|---|---|
| Extract | [`pipeline/extract.py`](pipeline/extract.py) | API pagination, polite concurrency, normalising two price schemas into one table |
| Idempotent loads | `partition_path()` + atomic file swap | Re-running a day overwrites only that day. Safe retries, safe backfills |
| Data quality gate | [`pipeline/checks.py`](pipeline/checks.py) | Row count, drop vs yesterday, null keys, % priced, source freshness |
| DAG | [`dags/pokemon_prices.py`](dags/pokemon_prices.py) | TaskFlow API, retries, failure callback, `catchup=False`, light top-level imports |
| Summary for the site | [`pipeline/summary.py`](pipeline/summary.py) | Priciest cards and biggest day-on-day movers, read live by my portfolio page |
| Alerts | [`pipeline/alerts.py`](pipeline/alerts.py) | Discord webhook on final failure; GitHub emails on a failed run |
| CI | [`.github/workflows/ci.yml`](.github/workflows/ci.yml) | Lint, unit tests, and a test that the DAG loads in real Airflow, on every PR |
| Scheduled run | [`.github/workflows/daily.yml`](.github/workflows/daily.yml) | Cron schedule, manual backfill by date, run fails if quality fails |

Merging to `main` ships the code to the next scheduled run (continuous delivery,
without a server to deploy to).

## Data

One row per card × marketplace × variant (normal, holo, reverse holo...):
`price_date, card_id, set_id, card_name, rarity, source, variant, currency,
low, mid, high, market, trend, avg7, avg30, source_updated_at`.

Sets tracked: *151*, *Prismatic Evolutions*, *Destined Rivals* (about 600 cards).
Prices come from the free [TCGdex](https://tcgdex.dev) API.

## Run it

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest -q                                   # unit tests (DAG test needs Airflow, Linux/macOS)
python -m pipeline.extract 2026-11-14 20    # extract 20 cards for one day
```

Backfill a day: **Actions → daily-pipeline → Run workflow**, then enter the date.

## Roadmap

- [x] Phase 1: Airflow DAG, Parquet lake partitioned by date, quality gate, alerts, CI
- [ ] Phase 2: dbt on DuckDB: staging, price-history snapshots, daily facts, a spike-detection test
- [ ] Phase 3: a live page with today's biggest price movers and pipeline health
