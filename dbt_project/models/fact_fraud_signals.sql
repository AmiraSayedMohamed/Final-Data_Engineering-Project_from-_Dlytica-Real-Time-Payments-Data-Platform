-- fact_fraud_signals: select transactions with fraud signals
select
  transaction_id,
  ts_event,
  card_hash,
  merchant_id,
  amount,
  currency,
  auth_result,
  flag_high_amount,
  flag_blacklisted
from {{ ref('fact_transactions') }}
where flag_high_amount = true or flag_blacklisted = true
