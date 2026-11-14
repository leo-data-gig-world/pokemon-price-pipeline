-- Daily price per series (card x marketplace x variant) with day-on-day change and a
-- z-score against the trailing 7 *calendar days*. A RANGE frame, not ROWS: if the
-- pipeline misses a day, "7 rows back" would silently stretch past a week.
with priced as (
    select * from {{ ref('stg_prices') }}
    where market is not null
),

windowed as (
    select
        *,
        lag(market) over by_day as prev_market,
        avg(market) over trailing_week as avg_prev_7d,
        stddev_samp(market) over trailing_week as sd_prev_7d,
        count(market) over trailing_week as n_prev_7d
    from priced
    window
        by_day as (partition by card_id, source, variant order by price_date),
        trailing_week as (
            partition by card_id, source, variant
            order by price_date
            range between interval 7 days preceding and interval 1 day preceding
        )
)

select
    *,
    round((market / nullif(prev_market, 0) - 1) * 100, 1) as day_change_pct,
    round((market - avg_prev_7d) / nullif(sd_prev_7d, 0), 2) as zscore_7d,
    coalesce(
        n_prev_7d >= {{ var('spike_min_days') }}
        and greatest(market, avg_prev_7d) >= {{ var('spike_min_price') }}
        and abs((market - avg_prev_7d) / nullif(sd_prev_7d, 0)) >= {{ var('spike_z') }},
        false
    ) as is_spike
from windowed
