You are a senior dbt and BigQuery data engineer. Build a production-grade dbt project 
implementing a Medallion Architecture (Raw → Silver → Gold) for a global ticketing 
platform analytics system.

---

## Business Context

We aggregate data from multiple ticketing platforms operating across different geographies 
(US, EU, APAC, LATAM). Each platform emits transactional data covering:

- **Event data**: event name, category, venue, date, capacity, geography
- **Customer data**: customer ID, region, signup date, loyalty tier (NOTE: PII fields 
  exist but are OUT OF SCOPE for this MVP — do not mask or encrypt yet, just ingest as-is)
- **Ticket data**: ticket ID, event ID, customer ID, ticket type, price, currency, 
  purchase timestamp, status (booked/cancelled/refunded)

The end goal is a sophisticated Business Intelligence layer consumed by key stakeholders 
and CIO/CTO dashboards — focusing on revenue trends, event performance, regional 
breakdowns, and customer behaviour.

---

## Business Context

We aggregate data from multiple ticketing platforms operating across different geographies
(US, EU, APAC, LATAM). Each platform emits transactional data through multiple channels —
including mobile apps, partner portals, the web platform, and third-party vendor 
integrations. This multi-channel, multi-platform nature means:

- The same event or customer may appear across multiple source channels
- Data quality and schema consistency varies by channel (e.g. third-party vendors 
  may send incomplete records)
- Channel attribution is critical for BI — stakeholders need to understand revenue 
  and ticket sales broken down by channel, not just by platform or region

The data covers:

- **Event data**: event name, category, venue, date, capacity, geography
- **Customer data**: customer ID, region, signup date, loyalty tier (NOTE: PII fields
  exist but are OUT OF SCOPE for this MVP — do not mask or encrypt yet, just ingest as-is)
- **Ticket data**: ticket ID, event ID, customer ID, ticket type, price, currency,
  purchase timestamp, status (booked/cancelled/refunded)
- **Channel/Source data**: the originating platform channel through which the ticket
  transaction or event record was emitted

The end goal is a sophisticated Business Intelligence layer consumed by key stakeholders
and CIO/CTO dashboards — focusing on revenue trends, event performance, regional
breakdowns, channel attribution, and customer behaviour.

---

## Raw Tables Already in BigQuery (dbt sources — do not recreate)

These tables exist in `raw_ticketing` dataset. Define them as dbt sources only:

1. `raw_ticketing.events`
   - Columns: event_id, event_name, event_category, venue_name, venue_city,
     venue_country, event_date, event_capacity, platform_source, ingested_at

2. `raw_ticketing.customers`
   - Columns: customer_id, first_name, last_name, email, phone, country_of_residence,
     signup_date, loyalty_tier, platform_source, ingested_at
   - NOTE: first_name, last_name, email, phone are PII — ingest as-is for MVP,
     flag with a comment: -- PII: to be masked in post-MVP

3. `raw_ticketing.tickets`
   - Columns: ticket_id, event_id, customer_id, ticket_type, face_value,
     currency, purchase_timestamp, ticket_status, discount_code, platform_source, 
     channel_id, ingested_at
   - NOTE: channel_id is a foreign key to raw_ticketing.channels

4. `raw_ticketing.platforms`
   - Columns: platform_id, platform_name, platform_region, launch_date, is_active

5. `raw_ticketing.channels`  ← NEW TABLE
   - Columns: 
       channel_id          -- surrogate key
       channel_name        -- e.g. 'Mobile App', 'Website', 'Partner Portal', 
                              'Third Party Vendor'
       channel_type        -- category: DIRECT | PARTNER | VENDOR
       channel_description -- free text describing the channel
       platform_id         -- FK to raw_ticketing.platforms (which platform owns 
                              this channel)
       is_active           -- BOOLEAN: whether the channel is currently live
       launch_date         -- DATE: when the channel went live
       ingested_at         -- TIMESTAMP: when this record landed in BigQuery

---

## Silver Layer addition — `silver_channels.sql`

Add to `models/silver/` alongside the existing silver models:

### `silver_channels.sql`
- Select from source('raw_ticketing', 'channels')
- Standardise channel_name to INITCAP()
- Standardise channel_type to UPPER() with accepted values: DIRECT, PARTNER, VENDOR
- Filter: WHERE is_active = TRUE
- Cast launch_date to DATE
- Add derived column: channel_age_days = DATE_DIFF(CURRENT_DATE, launch_date, DAY)
- Add _dbt_loaded_at = CURRENT_TIMESTAMP()
- Deduplicate on channel_id
- Materialise as: `incremental` using ingested_at as the incremental predicate

---

## Gold Layer updates — channel attribution added

Update the following existing gold models to incorporate channel breakdowns:

### `gold_ticket_sales_daily.sql` — add channel dimension
- Grain changes to: one row per event per day per platform per region per channel
- Add to JOIN: silver_channels via silver_tickets.channel_id
- Add to metrics: tickets_sold_by_channel, revenue_by_channel_usd
- Add columns: channel_name, channel_type

### `gold_regional_summary.sql` — add channel breakdown
- Add to metrics: top_channel_by_revenue, direct_vs_partner_vs_vendor_split (%)
- Join silver_channels via silver_tickets

### NEW gold model — `gold_channel_performance.sql`
- Grain: one row per channel per month
- Metrics:
    total_tickets_sold
    total_revenue_usd
    avg_ticket_price_usd
    cancellation_rate        -- cancelled / total tickets
    refund_rate              -- refunded / total tickets
    unique_customers
    unique_events
    channel_revenue_share_pct  -- this channel's % of total revenue that month
- Joins: silver_tickets + silver_channels + silver_events + silver_platforms
- Used by: CIO channel strategy dashboard and partner performance reviews
- Materialise as: `table`

---

## Testing additions for channels

### Source tests (add to sources.yml):
- not_null + unique on channel_id
- accepted_values on channel_type: ['DIRECT', 'PARTNER', 'VENDOR']
- not_null on platform_id, channel_name, channel_type

### Silver tests (add to schema.yml):
- not_null + unique on channel_id
- relationships: silver_channels.platform_id → silver_platforms.platform_id
- accepted_values on channel_type: ['DIRECT', 'PARTNER', 'VENDOR']

### Gold tests (add to schema.yml for gold_channel_performance):
- not_null on all metric columns
- Custom singular test `assert_channel_revenue_share_sums_to_100.sql`:
  SUM(channel_revenue_share_pct) grouped by month should equal 100

---

## Updated File Structure

ticketing_analytics/
├── dbt_project.yml
├── profiles.yml
├── packages.yml
├── run_pipeline.sh
├── seeds/
│   └── currency_rates.csv
├── sources/
│   └── sources.yml
├── models/
│   ├── silver/
│   │   ├── silver_events.sql
│   │   ├── silver_customers.sql
│   │   ├── silver_tickets.sql
│   │   ├── silver_platforms.sql
│   │   ├── silver_channels.sql        ← NEW
│   │   └── schema.yml
│   └── gold/
│       ├── gold_ticket_sales_daily.sql
│       ├── gold_event_performance.sql
│       ├── gold_customer_behaviour.sql
│       ├── gold_regional_summary.sql
│       ├── gold_channel_performance.sql  ← NEW
│       └── schema.yml
├── tests/
│   ├── assert_revenue_non_negative.sql
│   ├── assert_sell_through_rate.sql
│   └── assert_channel_revenue_share_sums_to_100.sql  ← NEW
└── macros/
    └── generate_schema_name.sql

---

## Non-Functional Requirements

- All SQL must be valid BigQuery Standard SQL
- All models must have descriptions in schema.yml (required for dbt docs)
- Use dbt_utils.generate_surrogate_key() for any composite keys in gold models
- Incremental models must handle late-arriving data (use `>= dateadd` lookback window)
- Add meta tags to schema.yml: owner, team, pii_present (true/false), layer
- No hardcoded project/dataset references in SQL — use dbt's `{{ source() }}` 
  and `{{ ref() }}` macros exclusively

---

Generate all files in full with working BigQuery SQL, inline comments, and 
a brief rationale comment block at the top of each SQL file explaining its 
purpose in the pipeline.