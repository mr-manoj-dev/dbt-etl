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

preferred_channels as (

  select
    customer_id,
    channel_id
  from (
    select
      customer_id,
      channel_id,
      row_number() over (partition by customer_id order by count(*) desc) as rn
    from tickets
    where ticket_status = 'booked' and channel_id is not null
    group by customer_id, channel_id
  )
  where rn = 1

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
    ch.channel_name                                         as preferred_channel,

    current_timestamp()                                     as _dbt_loaded_at

  from customers c
  left join tickets t
    on c.customer_id = t.customer_id
  left join preferred_channels pc
    on c.customer_id = pc.customer_id
  left join channels ch
    on pc.channel_id = ch.channel_id

  group by
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.country_of_residence,
    c.signup_date,
    c.loyalty_tier,
    c.platform_source,
    ch.channel_name

)

select * from customer_metrics
