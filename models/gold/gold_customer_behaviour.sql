/*
  ============================================================
  Model: gold_customer_behaviour
  Layer: Gold (Medallion Architecture)
  Purpose:
    Customer-level behavioural analytics for the CIO/CTO dashboards.
    Grain: one row per customer_id.
    Aggregates lifetime ticket purchase metrics, spend patterns, loyalty tier
    context, and preferred channel — enabling customer segmentation, churn
    modelling, and loyalty programme effectiveness analysis.
    NOTE: PII fields (first_name, last_name, email, phone) are joined from
    silver_customers as-is per MVP scope. Masking is planned for post-MVP.
  Materialisation: table (full refresh)
  ============================================================
*/

{{
  config(
    materialized='table',
    cluster_by=['loyalty_tier', 'country_of_residence'],
    tags=['gold', 'customer_behaviour'],
    meta={
      'owner': 'data_engineering',
      'team': 'platform',
      'pii_present': true,
      'layer': 'gold'
    }
  )
}}

with customers as (

  select * from {{ ref('silver_customers') }}

),

tickets as (

  select * from {{ ref('silver_tickets') }}

),

channels as (

  select * from {{ ref('silver_channels') }}

),

customer_metrics as (

  select
    c.customer_id,

    -- PII fields: to be masked in post-MVP
    c.first_name,   -- PII: to be masked in post-MVP
    c.last_name,    -- PII: to be masked in post-MVP
    c.email,        -- PII: to be masked in post-MVP
    c.phone,        -- PII: to be masked in post-MVP

    c.country_of_residence,
    c.signup_date,
    c.loyalty_tier,
    c.platform_source,

    -- Derived tenure metric
    date_diff(current_date(), c.signup_date, day)           as customer_tenure_days,

    -- Ticket volume KPIs
    count(t.ticket_id)                                      as total_ticket_events,
    countif(t.ticket_status = 'booked')                     as total_tickets_purchased,
    countif(t.ticket_status = 'cancelled')                  as total_cancellations,
    countif(t.ticket_status = 'refunded')                   as total_refunds,

    -- Spend KPIs (USD)
    round(sum(case when t.ticket_status = 'booked'
              then t.face_value_usd else 0 end), 2)         as lifetime_spend_usd,
    round(avg(case when t.ticket_status = 'booked'
              then t.face_value_usd end), 2)                as avg_spend_per_ticket_usd,

    -- Recency
    max(t.purchase_date)                                    as last_purchase_date,
    date_diff(current_date(), max(t.purchase_date), day)    as days_since_last_purchase,

    -- Event diversity
    count(distinct t.event_id)                              as unique_events_attended,

    -- Channel preference: the channel with the most booked tickets
    -- (uses ARRAY_AGG with ORDER BY to get the top channel name)
    (
      select ch.channel_name
      from unnest(
        array_agg(
          struct(t2.channel_id, count(*) as cnt)
          order by count(*) desc
          limit 1
        )
      ) as top
      join {{ ref('silver_channels') }} ch on ch.channel_id = top.channel_id
    )                                                       as preferred_channel,

    current_timestamp()                                     as _dbt_loaded_at

  from customers c
  left join tickets t
    on c.customer_id = t.customer_id

  group by
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.country_of_residence,
    c.signup_date,
    c.loyalty_tier,
    c.platform_source

)

select * from customer_metrics
