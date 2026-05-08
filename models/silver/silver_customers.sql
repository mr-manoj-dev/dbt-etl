/*
  ============================================================
  Model: silver_customers
  Layer: Silver (Medallion Architecture)
  Purpose:
    Cleans and standardises raw customer records from raw_ticketing.customers.
    Deduplicates on customer_id keeping the latest record. Standardises loyalty
    tiers and geographic fields. PII fields (first_name, last_name, email, phone)
    are ingested as-is per MVP scope — masking is planned for post-MVP.
    This model is the single customer dimension consumed by gold models.
  Materialisation: incremental (merge on customer_id)
  Incremental strategy: 3-day lookback on ingested_at for late-arriving data.
  ============================================================
*/

{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='customer_id',
    cluster_by=['country_of_residence', 'loyalty_tier'],
    tags=['silver', 'customers'],
    meta={
      'owner': 'data_engineering',
      'team': 'platform',
      'pii_present': true,
      'layer': 'silver'
    }
  )
}}

with source as (

  select * from {{ source('raw_ticketing', 'customers') }}

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
      partition by customer_id
      order by ingested_at desc
    ) as _row_num

  from source

),

cleaned as (

  select
    customer_id,

    -- PII: to be masked in post-MVP
    first_name,  -- PII: to be masked in post-MVP
    last_name,   -- PII: to be masked in post-MVP
    email,       -- PII: to be masked in post-MVP
    phone,       -- PII: to be masked in post-MVP

    -- Standardised geographic and categorical fields
    upper(trim(country_of_residence))                      as country_of_residence,
    cast(signup_date as date)                              as signup_date,
    upper(trim(loyalty_tier))                              as loyalty_tier,
    upper(trim(platform_source))                           as platform_source,

    ingested_at,
    current_timestamp()                                    as _dbt_loaded_at

  from deduplicated
  where _row_num = 1
    and customer_id is not null

)

select * from cleaned
