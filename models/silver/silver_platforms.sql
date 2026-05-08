/*
  ============================================================
  Model: silver_platforms
  Layer: Silver (Medallion Architecture)
  Purpose:
    Cleans and standardises the raw_ticketing.platforms reference table.
    Platforms are a slowly-changing dimension (SCD-1) — only the current
    state is retained. Used as a dimension in gold models to provide
    regional context for ticket sales and channel attribution.
  Materialisation: table (small reference dataset — full refresh acceptable)
  ============================================================
*/

{{
  config(
    materialized='table',
    tags=['silver', 'platforms', 'reference'],
    meta={
      'owner': 'data_engineering',
      'team': 'platform',
      'pii_present': false,
      'layer': 'silver'
    }
  )
}}

with source as (

  select * from {{ source('raw_ticketing', 'platforms') }}

),

deduplicated as (

  select
    *,
    row_number() over (
      partition by platform_id
      order by platform_id  -- stable ordering; no ingested_at on platforms
    ) as _row_num

  from source

),

cleaned as (

  select
    platform_id,
    initcap(trim(platform_name))    as platform_name,
    upper(trim(platform_region))    as platform_region,
    cast(launch_date as date)       as launch_date,
    cast(is_active as bool)         as is_active,
    current_timestamp()             as _dbt_loaded_at

  from deduplicated
  where _row_num = 1
    and platform_id is not null

)

select * from cleaned
