/*
  ============================================================
  Model: gold_ticket_sales_daily
  Layer: Gold (Medallion Architecture)
  Purpose:
    Aggregated daily ticket sales fact table for the BI layer.
    Grain: one row per event × day × platform × region × channel.
    Provides the primary revenue and volume metrics consumed by:
      - CIO/CTO revenue dashboards
      - Daily sales operations reports
      - Channel attribution analysis
    Includes channel dimension (channel_name, channel_type) to enable
    full channel-level revenue and volume breakdowns.
  Materialisation: table (full refresh — pre-aggregated for BI performance)
  ============================================================
*/

{{
  config(
    materialized='table',
    partition_by={
      'field': 'sale_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    cluster_by=['platform_region', 'channel_type'],
    tags=['gold', 'ticket_sales', 'daily'],
    meta={
      'owner': 'data_engineering',
      'team': 'platform',
      'pii_present': false,
      'layer': 'gold'
    }
  )
}}

with tickets as (

  select * from {{ ref('silver_tickets') }}

),

events as (

  select * from {{ ref('silver_events') }}

),

platforms as (

  select * from {{ ref('silver_platforms') }}

),

channels as (

  select * from {{ ref('silver_channels') }}

),

joined as (

  select
    -- Grain dimensions
    t.purchase_date                           as sale_date,
    e.event_id,
    e.event_name,
    e.event_category,
    e.venue_country,
    p.platform_id,
    p.platform_name,
    p.platform_region,
    c.channel_id,
    c.channel_name,
    c.channel_type,

    -- Surrogate key for the grain (used for BI tool joins)
    {{ dbt_utils.generate_surrogate_key([
        't.purchase_date',
        'e.event_id',
        'p.platform_id',
        'e.venue_country',
        'c.channel_id'
    ]) }}                                     as daily_sales_key,

    -- Volume metrics
    countif(t.ticket_status = 'booked')       as tickets_sold,
    countif(t.ticket_status = 'cancelled')    as tickets_cancelled,
    countif(t.ticket_status = 'refunded')     as tickets_refunded,
    count(t.ticket_id)                        as total_ticket_events,

    -- Revenue metrics (USD normalised)
    round(sum(
      case when t.ticket_status = 'booked'
      then t.face_value_usd else 0 end
    ), 2)                                     as revenue_usd,

    round(sum(
      case when t.ticket_status = 'refunded'
      then t.face_value_usd else 0 end
    ), 2)                                     as refund_value_usd,

    -- Channel-specific breakdowns
    countif(t.ticket_status = 'booked')       as tickets_sold_by_channel,
    round(sum(
      case when t.ticket_status = 'booked'
      then t.face_value_usd else 0 end
    ), 2)                                     as revenue_by_channel_usd,

    count(distinct t.customer_id)             as unique_customers,

    current_timestamp()                       as _dbt_loaded_at

  from tickets t
  inner join events e
    on t.event_id = e.event_id
  inner join platforms p
    on t.platform_source = p.platform_name  -- join on platform name from tickets
  left join channels c
    on t.channel_id = c.channel_id

  group by
    t.purchase_date,
    e.event_id,
    e.event_name,
    e.event_category,
    e.venue_country,
    p.platform_id,
    p.platform_name,
    p.platform_region,
    c.channel_id,
    c.channel_name,
    c.channel_type

)

select * from joined
