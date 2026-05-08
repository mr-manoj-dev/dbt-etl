/*
  ============================================================
  Model: silver_tickets
  Layer: Silver (Medallion Architecture)
  Purpose:
    Cleans and standardises raw ticket transaction records from raw_ticketing.tickets.
    Deduplicates on ticket_id keeping the latest ingested version (SCD-1 approach).
    Converts face_value to USD using the currency_rates seed for a unified
    monetary metric. Retains channel_id as a foreign key consumed by gold models.
    This model is the central fact table in the Silver layer, joining to all
    other silver dimensions at the Gold layer.
  Materialisation: incremental (merge on ticket_id)
  Incremental strategy: 3-day lookback on purchase_timestamp for late arrivals.
  ============================================================
*/

{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ticket_id',
    partition_by={
      'field': 'purchase_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    cluster_by=['ticket_status', 'platform_source'],
    tags=['silver', 'tickets'],
    meta={
      'owner': 'data_engineering',
      'team': 'platform',
      'pii_present': false,
      'layer': 'silver'
    }
  )
}}

with source as (

  select * from {{ source('raw_ticketing', 'tickets') }}

  {% if is_incremental() %}
    where ingested_at >= timestamp_sub(
      (select max(ingested_at) from {{ this }}),
      interval 3 day
    )
  {% endif %}

),

deduplicated as (

  select
    *,
    row_number() over (
      partition by ticket_id
      order by ingested_at desc
    ) as _row_num

  from source

),

-- Join currency rates seed to normalise all prices to USD
currency_rates as (

  select
    from_currency,
    to_currency,
    rate,
    effective_date
  from {{ ref('currency_rates') }}
  where to_currency = 'USD'

),

cleaned as (

  select
    t.ticket_id,
    t.event_id,
    t.customer_id,
    t.channel_id,

    upper(trim(t.ticket_type))     as ticket_type,
    cast(t.face_value as numeric)  as face_value,
    upper(trim(t.currency))        as currency,

    -- Normalise to USD; fall back to 1.0 if USD source (no conversion needed)
    round(
      cast(t.face_value as numeric) * coalesce(cr.rate, 1.0),
      2
    )                              as face_value_usd,

    cast(t.purchase_timestamp as timestamp)       as purchase_timestamp,
    date(t.purchase_timestamp)                    as purchase_date,
    lower(trim(t.ticket_status))                  as ticket_status,
    t.discount_code,
    upper(trim(t.platform_source))                as platform_source,
    t.ingested_at,

    current_timestamp()                           as _dbt_loaded_at

  from deduplicated t
  left join currency_rates cr
    on upper(trim(t.currency)) = cr.from_currency

  where t._row_num = 1
    and t.ticket_id   is not null
    and t.event_id    is not null
    and t.customer_id is not null

)

select * from cleaned
