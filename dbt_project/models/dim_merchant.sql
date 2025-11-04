-- build dim_merchant from staging
select distinct
  merchant_id,
  mcc
from {{ ref('stg_payments') }}
where merchant_id is not null
-- simple dim_merchant model
select
    merchant_id,
    mcc
from raw.fact_transactions
