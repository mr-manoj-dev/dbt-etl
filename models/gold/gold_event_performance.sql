/*
  ============================================================
  Model: gold_event_performance
  Layer: Gold (Medallion Architecture)
  Purpose:
    Event-level performance summary for BI dashboards.
    Grain: one row per event_id.
    Provides event sell-through rates, total revenue, cancellation/refund
    rates, and channel diversification metrics — enabling event managers
    and regional leads to benchmark event commercial performance.
  Materialisation: table (full refresh)
  ============================================================
*/

{{
  config(
    materialized='table',
    cluster_by=['event_category', 'venue_country'],
    tags=['gold', 'event_performance'],
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

channels as (

  select * from {{ ref('silver_channels') }}

),

event_metrics as (

  select
    e.event_id,
    e.event_name,
    e.event_category,
    e.venue_name,
    e.venue_city,
    e.venue_country,
    e.event_date,
    e.event_capacity,
    e.platform_source,

    -- Volume KPIs
    count(t.ticket_id)                                      as total_ticket_events,
    countif(t.ticket_status = 'booked')                     as tickets_sold,
    countif(t.ticket_status = 'cancelled')                  as tickets_cancelled,
    countif(t.ticket_status = 'refunded')                   as tickets_refunded,

    -- Revenue KPIs (USD)
    round(sum(case when t.ticket_status = 'booked'
              then t.face_value_usd else 0 end), 2)         as total_revenue_usd,
    round(avg(case when t.ticket_status = 'booked'
              then t.face_value_usd end), 2)                as avg_ticket_price_usd,

    -- Performance ratios
    round(
      safe_divide(
        countif(t.ticket_status = 'booked'),
        nullif(e.event_capacity, 0)
      ) * 100, 2
    )                                                       as sell_through_rate_pct,

    round(
      safe_divide(
        countif(t.ticket_status = 'cancelled'),
        nullif(count(t.ticket_id), 0)
      ) * 100, 2
    )                                                       as cancellation_rate_pct,

    round(
      safe_divide(
        countif(t.ticket_status = 'refunded'),
        nullif(count(t.ticket_id), 0)
      ) * 100, 2
    )                                                       as refund_rate_pct,

    -- Customer reach
    count(distinct t.customer_id)                           as unique_customers,

    -- Channel diversification: how many distinct channels sold tickets to this event
    count(distinct t.channel_id)                            as channel_count,

    current_timestamp()                                     as _dbt_loaded_at

  from events e
  left join tickets t
    on e.event_id = t.event_id

  group by
    e.event_id,
    e.event_name,
    e.event_category,
    e.venue_name,
    e.venue_city,
    e.venue_country,
    e.event_date,
    e.event_capacity,
    e.platform_source

)

select * from event_metrics
