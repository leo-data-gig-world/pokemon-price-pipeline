-- Price spikes, written back to the lake as Parquet so Airflow can alert on them
-- and the portfolio site can show them.
{{ config(materialized='external', location=env_var('LAKE_DIR', 'lake') ~ '/marts/price_spikes.parquet') }}

select
    price_date,
    card_id,
    card_name,
    set_id,
    source,
    variant,
    currency,
    market,
    round(avg_prev_7d, 2) as avg_prev_7d,
    zscore_7d,
    day_change_pct
from {{ ref('fct_daily_prices') }}
where is_spike
order by price_date desc, abs(zscore_7d) desc
