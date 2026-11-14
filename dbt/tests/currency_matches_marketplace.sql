-- Cardmarket quotes EUR and TCGplayer quotes USD. A mismatch means the extract
-- mapped a price into the wrong column or the API changed shape.
select source, currency, count(*) as n
from {{ ref('stg_prices') }}
where (source = 'cardmarket' and currency <> 'EUR')
   or (source = 'tcgplayer' and currency <> 'USD')
group by 1, 2
