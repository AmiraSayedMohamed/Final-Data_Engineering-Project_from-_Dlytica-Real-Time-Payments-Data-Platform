{% snapshot merchant_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='merchant_id',
    strategy='timestamp',
    updated_at='ts_event'
) }}

select
  merchant_id,
  mcc
from {{ ref('stg_payments') }}
where merchant_id is not null

{% endsnapshot %}
