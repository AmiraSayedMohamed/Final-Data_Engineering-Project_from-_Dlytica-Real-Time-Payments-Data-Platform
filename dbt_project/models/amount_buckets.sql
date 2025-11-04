-- amount_buckets: categorize transactions into buckets
select
  transaction_id,
  amount,
  case
    when amount < 10 then '<10'
    when amount >= 10 and amount < 100 then '10-99'
    when amount >= 100 and amount < 1000 then '100-999'
    when amount >= 1000 and amount < 10000 then '1000-9999'
    else '10000+'
  end as amount_bucket
from {{ ref('fact_transactions') }}
