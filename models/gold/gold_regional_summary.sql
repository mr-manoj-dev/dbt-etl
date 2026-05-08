/*
  ============================================================
  Model: gold_regional_summary
  Layer: Gold (Medallion Architecture)
  Purpose:
    Regional roll-up of ticket sales performance for executive dashboards.
    Grain: one row per platform_region × year_month.
    Provides regional revenue trends, event volumes, customer acquisition,
    and full channel attribution breakdown (DIRECT vs PARTNER vs VENDOR split)
    for the CIO/CTO strategy layer.
    Updated to include:
      - top_channel_by_revenue: the channel generating most revenue in the region/month
      - direct_vs_partner_vs_vendor_split: percentage revenue contribution by channel type
  Materialisation: table (full refresh — aggregated for BI performance)
  ============================================================
*/

{{
  config(
    materialized='table',
    cluster_by=['platform_region'],
    tags=['gold', 'regional_summary'],
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
    p.platform_region,
    format_date('%Y-%m', t.purchase_date)        as year_month,
    c.channel_id,
    c.channel_name,
    c.channel_type,
    t.ticket_status,
    t.face_value_usd,
    t.ticket_id,
    t.customer_id,
    t.event_id

  from tickets t
  inner join events e     on t.event_id = e.event_id
  inner join platforms p  on t.platform_source = p.platform_name
  left  join channels c   on t.channel_id = c.channel_id

),

regional_base as (

  select
    platform_region,
    year_month,

    -- Volume KPIs
    countif(ticket_status = 'booked')                                       as tickets_sold,
    countif(ticket_status = 'cancelled')                                    as tickets_cancelled,
    countif(ticket_status = 'refunded')                                     as tickets_refunded,

    -- Revenue KPIs (USD)
    round(sum(case when ticket_status = 'booked'
              then face_value_usd else 0 end), 2)                           as total_revenue_usd,

    -- Channel type revenue splits (for percentage calculation below)
    round(sum(case when ticket_status = 'booked' and channel_type = 'DIRECT'
              then face_value_usd else 0 end), 2)                           as direct_revenue_usd,
    round(sum(case when ticket_status = 'booked' and channel_type = 'PARTNER'
              then face_value_usd else 0 end), 2)                           as partner_revenue_usd,
    round(sum(case when ticket_status = 'booked' and channel_type = 'VENDOR'
              then face_value_usd else 0 end), 2)                           as vendor_revenue_usd,

    -- Reach KPIs
    count(distinct customer_id)                                             as unique_customers,
    count(distinct event_id)                                                as unique_events,

    -- Channel count
    count(distinct channel_id)                                              as active_channels

  from joined
  group by platform_region, year_month

),

-- Derive top_channel_by_revenue per region/month
channel_ranked as (

  select
    platform_region,
    year_month,
    channel_name,
    sum(case when ticket_status = 'booked' then face_value_usd else 0 end) as channel_rev,
    row_number() over (
      partition by platform_region, year_month
      order by sum(case when ticket_status = 'booked' then face_value_usd else 0 end) desc
    ) as rn

  from joined
  group by platform_region, year_month, channel_name

),

top_channel as (

  select platform_region, year_month, channel_name as top_channel_by_revenue
  from channel_ranked
  where rn = 1

),

final as (

  select
    rb.platform_region,
    rb.year_month,
    rb.tickets_sold,
    rb.tickets_cancelled,
    rb.tickets_refunded,
    rb.total_revenue_usd,
    rb.unique_customers,
    rb.unique_events,
    rb.active_channels,

    tc.top_channel_by_revenue,

    -- Channel type revenue split percentages
    round(safe_divide(rb.direct_revenue_usd,  rb.total_revenue_usd) * 100, 2) as direct_revenue_pct,
    round(safe_divide(rb.partner_revenue_usd, rb.total_revenue_usd) * 100, 2) as partner_revenue_pct,
    round(safe_divide(rb.vendor_revenue_usd,  rb.total_revenue_usd) * 100, 2) as vendor_revenue_pct,

    -- Direct vs Partner vs Vendor split as a formatted label for BI tools
    concat(
      'DIRECT: ',   cast(round(safe_divide(rb.direct_revenue_usd,  rb.total_revenue_usd) * 100, 1) as string), '% | ',
      'PARTNER: ',  cast(round(safe_divide(rb.partner_revenue_usd, rb.total_revenue_usd) * 100, 1) as string), '% | ',
      'VENDOR: ',   cast(round(safe_divide(rb.vendor_revenue_usd,  rb.total_revenue_usd) * 100, 1) as string), '%'
    )                                                                           as direct_vs_partner_vs_vendor_split,

    current_timestamp()                                                         as _dbt_loaded_at

  from regional_base rb
  left join top_channel tc
    on rb.platform_region = tc.platform_region
    and rb.year_month = tc.year_month

)

select * from final
