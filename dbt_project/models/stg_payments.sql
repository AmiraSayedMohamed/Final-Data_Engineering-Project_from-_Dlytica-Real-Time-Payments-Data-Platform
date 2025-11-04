-- staging payments based on seed `payments_raw`
with raw as (
    select * from {{ ref('payments_raw') }}
)

select
    transaction_id,
    cast(ts_event as timestamp) as ts_event,
    card_hash,
    merchant_id,
    amount,
    currency,
    mcc,
    channel,
    auth_result,
    location
from raw
