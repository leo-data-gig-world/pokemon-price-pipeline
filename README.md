# Pokémon card price pipeline

[![daily-pipeline](https://github.com/leo-data-gig-world/pokemon-price-pipeline/actions/workflows/daily.yml/badge.svg)](https://github.com/leo-data-gig-world/pokemon-price-pipeline/actions/workflows/daily.yml) [![ci](https://github.com/leo-data-gig-world/pokemon-price-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/leo-data-gig-world/pokemon-price-pipeline/actions/workflows/ci.yml)

A small, real data pipeline: every morning it pulls Pokémon card prices from two
marketplaces, checks the data, and stores one partition per day in a Parquet data lake.

Designed around the parts of data engineering that matter in production:
**orchestration, idempotent loads, backfills, data quality gates, alerting and CI/CD**.

```
                      Airflow DAG  pokemon_prices  (daily 06:30 UTC)
 TCGdex API ──► extract_prices ──► check_quality ──► dbt_build ──┬──► publish_summary ──► site summary
 (Cardmarket EUR,        │                                 │       └──► alert_spikes ────► Discord / site
  TCGplayer USD)         ▼                                 ▼
     lake/raw/prices/date=YYYY-MM-DD/      dbt on DuckDB: stg → fct_daily_prices → price_spikes
                                           (any task fails: retry x2, then alert)
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
| dbt models | [`dbt/`](dbt) | DuckDB reading the Parquet lake directly; staging → fact with trailing-week stats → spikes exported back to the lake |
| Spike detection | [`fct_daily_prices.sql`](dbt/models/marts/fct_daily_prices.sql) | z-score vs a 7-*calendar*-day RANGE window (not ROWS, so a missed day can't stretch the window), with minimum-history and minimum-price guards |
| dbt tests | [`_marts.yml`](dbt/models/marts/_marts.yml) | Unique/not-null keys, accepted values, a custom `positive` test, a currency-vs-marketplace check, and a **unit test** for the spike logic |
| Spike alerts | [`pipeline/spikes.py`](pipeline/spikes.py) | Alerts on today's spikes and publishes them for the site |
| Summary for the site | [`pipeline/summary.py`](pipeline/summary.py) | Priciest cards and biggest day-on-day movers, read live by my portfolio page |
| Run monitoring | [`pipeline/metrics.py`](pipeline/metrics.py) | Every stage records its timing and result, failed runs included; `history.json` feeds the [monitoring page](https://leo-data-gig-world.github.io/monitoring/) |
| Alerts | [`pipeline/alerts.py`](pipeline/alerts.py) | Discord webhook on final failure; GitHub emails on a failed run |
| CI | [`.github/workflows/ci.yml`](.github/workflows/ci.yml) | Lint, unit tests, and a test that the DAG loads in real Airflow, on every PR |
| dbt docs | [live site](https://leo-data-gig-world.github.io/pokemon-price-pipeline/) | Models, tests and lineage graph, rebuilt and published by every daily run |
| Scheduled run | [`.github/workflows/daily.yml`](.github/workflows/daily.yml) | Cron schedule, manual backfill by date, run fails if quality fails |

dbt runs in its own virtualenv, so its dependencies never fight Airflow's pinned
constraints. The warehouse is rebuilt from the immutable raw lake on every run: at
this size a full rebuild is cheaper and simpler than incremental models, and it makes
any logic change apply to all history at once.

Merging to `main` ships the code to the next scheduled run (continuous delivery,
without a server to deploy to).

## Monitoring

**Live dashboard: https://leo-data-gig-world.github.io/monitoring/**

Every run writes `lake/_health/runs/<date>.json`: each stage's duration and result, rows
loaded, quality checks, dbt test counts (from dbt's own `run_results.json`) and spikes found.
A final task runs even when an earlier one fails, so a failed day is recorded, not lost,
and a day with no record at all shows up as a missed run.

Scheduled GitHub Actions runs can be delayed or dropped under load (it happened on day 2),
so a backup schedule runs at 10:30 UTC and exits early if the 06:30 run already passed.
Re-running a day is safe because every load is idempotent.

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
- [x] Phase 2: dbt on DuckDB: staging, daily facts with trailing-week stats, spike detection + alerts, unit tests
- [ ] Phase 3: a live page with today's biggest price movers and pipeline health
