-- build dim_card from staging
select distinct
  card_hash
from {{ ref('stg_payments') }}
where card_hash is not null
-- dim_card model
select
  card_hash,
  md5(card_hash) as card_fingerprint
from raw.fact_transactions
