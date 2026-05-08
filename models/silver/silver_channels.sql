/*
  ============================================================
  Model: silver_channels
  Layer: Silver (Medallion Architecture)
  Purpose:
    Cleans and standardises the raw_ticketing.channels reference table.
    Channels represent the sales pipeline through which tickets reach customers:
    mobile apps, websites, partner portals, and third-party vendor integrations.
    This model:
      - Standardises channel_name to INITCAP and channel_type to UPPER
      - Filters to only active channels (is_active = TRUE)
      - Derives channel_age_days for operational dashboards
      - Deduplicates on channel_id keeping the latest ingested record
    Downstream consumers: silver_tickets (via channel_id FK), all gold channel models.
  Materialisation: incremental (merge on channel_id)
  Incremental strategy: 3-day lookback on ingested_at for late-arriving records.
  ============================================================
*/

{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='channel_id',
    tags=['silver', 'channels', 'reference'],
    meta={
      'owner': 'data_engineering',
      'team': 'platform',
      'pii_present': false,
      'layer': 'silver'
    }
  )
}}

with source as (

  select * from {{ source('raw_ticketing', 'channels') }}

  {% if is_incremental() %}
    -- 3-day lookback to handle late-arriving channel reference updates
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
      partition by channel_id
      order by ingested_at desc
    ) as _row_num

  from source

),

cleaned as (

  select
    channel_id,

    -- Standardise display text
    initcap(trim(channel_name))          as channel_name,

    -- Enforce controlled vocabulary for channel_type
    upper(trim(channel_type))            as channel_type,

    trim(channel_description)           as channel_description,
    platform_id,

    cast(is_active as bool)             as is_active,
    cast(launch_date as date)           as launch_date,

    -- Derived operational metric: how many days has this channel been live?
    date_diff(current_date(), cast(launch_date as date), day) as channel_age_days,

    ingested_at,
    current_timestamp()                 as _dbt_loaded_at

  from deduplicated
  where _row_num = 1
    -- Only expose active channels to downstream consumers
    and is_active = true
    and channel_id is not null

)

select * from cleaned
