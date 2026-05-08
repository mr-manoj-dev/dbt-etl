/*
  ============================================================
  Singular Test: assert_sell_through_rate
  Purpose:
    Asserts that no event's sell-through rate exceeds 100%.
    A sell_through_rate_pct > 100 would imply more tickets were sold
    than the venue's stated capacity — a data quality or capacity
    modelling error that would mislead event performance dashboards.
  Failure condition: any row returned = test fails.
  ============================================================
*/

select
  event_id,
  event_name,
  tickets_sold,
  event_capacity,
  sell_through_rate_pct
from {{ ref('gold_event_performance') }}
where sell_through_rate_pct > 100
  and event_capacity is not null
  and event_capacity > 0
