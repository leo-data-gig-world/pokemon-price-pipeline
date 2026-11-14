-- One row per card x marketplace x variant x day, typed and keyed.
select
    md5(concat_ws('|', card_id, source, variant, cast(price_date as varchar))) as price_key,
    cast(price_date as date) as price_date,
    card_id,
    set_id,
    card_name,
    rarity,
    source,
    variant,
    currency,
    low,
    mid,
    high,
    market,
    trend,
    avg7,
    avg30,
    cast(source_updated_at as timestamptz) as source_updated_at
from {{ source('lake', 'prices') }}
