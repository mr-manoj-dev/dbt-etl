/*
  ============================================================
  Model: gold_channel_performance
  Layer: Gold (Medallion Architecture)
  Purpose:
    Monthly channel performance summary for executive channel strategy dashboards.
    Grain: one row per channel_id × year_month.
    Powers the CIO channel strategy dashboard and partner performance reviews.
    Provides:
      - Volume KPIs: tickets sold, unique customers, unique events
      - Revenue KPIs: total, average ticket price (USD)
      - Quality KPIs: cancellation rate, refund rate
      - Share KPI: this channel's % of total monthly platform revenue
    channel_revenue_share_pct sums to 100% across all channels for a given month
    (validated by singular test assert_channel_revenue_share_sums_to_100.sql).
  Materialisation: table (full refresh — confirmed in MVP plan)
  ============================================================
*/

{{
  config(
    materialized='table',
    cluster_by=['channel_type'],
    tags=['gold', 'channel_performance'],
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

channels as (

  select * from {{ ref('silver_channels') }}

),

events as (

  select * from {{ ref('silver_events') }}

),

platforms as (

  select * from {{ ref('silver_platforms') }}

),

joined as (

  select
    format_date('%Y-%m', t.purchase_date)    as year_month,
    c.channel_id,
    c.channel_name,
    c.channel_type,
    c.platform_id,
    p.platform_name,
    p.platform_region,

    t.ticket_id,
    t.ticket_status,
    t.face_value_usd,
    t.customer_id,
    t.event_id

  from tickets t
  inner join channels c   on t.channel_id = c.channel_id
  inner join events e     on t.event_id = e.event_id
  inner join platforms p  on c.platform_id = p.platform_id

),

channel_monthly as (

  select
    year_month,
    channel_id,
    channel_name,
    channel_type,
    platform_id,
    platform_name,
    platform_region,

    -- Volume KPIs
    count(ticket_id)                                                         as total_ticket_events,
    countif(ticket_status = 'booked')                                        as total_tickets_sold,
    countif(ticket_status = 'cancelled')                                     as total_cancelled,
    countif(ticket_status = 'refunded')                                      as total_refunded,

    -- Revenue KPIs (USD)
    round(sum(case when ticket_status = 'booked'
              then face_value_usd else 0 end), 2)                            as total_revenue_usd,

    round(avg(case when ticket_status = 'booked'
              then face_value_usd end), 2)                                   as avg_ticket_price_usd,

    -- Quality KPIs
    round(
      safe_divide(
        countif(ticket_status = 'cancelled'),
        nullif(count(ticket_id), 0)
      ) * 100, 2
    )                                                                        as cancellation_rate,

    round(
      safe_divide(
        countif(ticket_status = 'refunded'),
        nullif(count(ticket_id), 0)
      ) * 100, 2
    )                                                                        as refund_rate,

    -- Reach KPIs
    count(distinct customer_id)                                              as unique_customers,
    count(distinct event_id)                                                 as unique_events,

    current_timestamp()                                                      as _dbt_loaded_at

  from joined
  group by
    year_month,
    channel_id,
    channel_name,
    channel_type,
    platform_id,
    platform_name,
    platform_region

),

-- Calculate total monthly revenue across all channels for share calculation
monthly_totals as (

  select
    year_month,
    sum(total_revenue_usd) as platform_total_revenue_usd

  from channel_monthly
  group by year_month

),

final as (

  select
    cm.year_month,
    cm.channel_id,
    cm.channel_name,
    cm.channel_type,
    cm.platform_id,
    cm.platform_name,
    cm.platform_region,

    cm.total_tickets_sold,
    cm.total_cancelled,
    cm.total_refunded,
    cm.total_revenue_usd,
    cm.avg_ticket_price_usd,
    cm.cancellation_rate,
    cm.refund_rate,
    cm.unique_customers,
    cm.unique_events,

    -- Channel revenue share: this channel's % of total monthly revenue across all channels
    round(
      safe_divide(cm.total_revenue_usd, mt.platform_total_revenue_usd) * 100,
      4
    )                                                                        as channel_revenue_share_pct,

    cm._dbt_loaded_at

  from channel_monthly cm
  inner join monthly_totals mt
    on cm.year_month = mt.year_month

)

select * from final
