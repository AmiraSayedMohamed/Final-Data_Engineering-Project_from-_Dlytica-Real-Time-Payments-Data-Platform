-- settlement_daily: sums per merchant, currency, day with simple fee calculation
with tx as (
  select
    merchant_id,
    currency,
    date_trunc('day', ts_event)::date as dt,
    amount
  from {{ ref('fact_transactions') }}
)

select
  merchant_id,
  currency,
  dt,
  count(*) as txn_count,
  sum(amount) as gross_amount,
  sum(amount) * 0.02 as fees, -- simple 2% fee for demo
  sum(amount) - sum(amount) * 0.02 as net_amount
from tx
group by merchant_id, currency, dt
