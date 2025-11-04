-- fact_transactions model
select
  p.transaction_id,
  p.ts_event,
  p.card_hash,
  p.merchant_id,
  p.amount,
  p.currency,
  p.auth_result,
  case when p.amount > 10000 then true else false end as flag_high_amount,
  case when b.merchant_id is not null then true else false end as flag_blacklisted
from {{ ref('stg_payments') }} p
left join {{ ref('blacklist_merchants') }} b on p.merchant_id = b.merchant_id
