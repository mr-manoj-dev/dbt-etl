/*
  ============================================================
  Model: silver_events
  Layer: Silver (Medallion Architecture)
  Purpose:
    Cleans and standardises raw event records from raw_ticketing.events.
    Deduplicates on event_id keeping the latest ingestion, casts data types,
    and applies lightweight filtering to remove clearly invalid records.
    This model acts as the single source of truth for event dimensions
    used by all downstream gold models.
  Materialisation: incremental (append-only on ingested_at)
  Incremental strategy: late-arriving data handled via 3-day lookback window.
  ============================================================
*/

{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    cluster_by=['venue_country', 'event_date'],
    tags=['silver', 'events'],
    meta={
      'owner': 'data_engineering',
      'team': 'platform',
      'pii_present': false,
      'layer': 'silver'
    }
  )
}}

with source as (

  select * from {{ source('raw_ticketing', 'events') }}

  {% if is_incremental() %}
    -- Lookback 3 days to capture late-arriving records and handle reprocessing
    where ingested_at >= timestamp_sub(
      (select max(ingested_at) from {{ this }}),
      interval 3 day
    )
  {% endif %}

),

deduplicated as (

  select
    event_id,
    event_name,
    event_category,
    venue_name,
    venue_city,
    venue_country,
    event_date,
    event_capacity,
    platform_source,
    ingested_at,
    -- Row number to deduplicate: keep the latest ingested record per event_id
    row_number() over (
      partition by event_id
      order by ingested_at desc
    ) as _row_num

  from source

),

cleaned as (

  select
    event_id,

    -- Standardise text fields
    initcap(trim(event_name))     as event_name,
    initcap(trim(event_category)) as event_category,
    initcap(trim(venue_name))     as venue_name,
    initcap(trim(venue_city))     as venue_city,
    upper(trim(venue_country))    as venue_country,

    -- Type-safe casts
    cast(event_date as date)          as event_date,
    cast(event_capacity as int64)     as event_capacity,

    upper(trim(platform_source))      as platform_source,
    ingested_at,

    -- Audit column
    current_timestamp() as _dbt_loaded_at

  from deduplicated
  where _row_num = 1
    -- Filter out records with no valid event date
    and event_date is not null
    and event_id is not null

)

select * from cleaned
