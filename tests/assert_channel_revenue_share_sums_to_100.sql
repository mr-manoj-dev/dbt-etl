/*
  ============================================================
  Singular Test: assert_channel_revenue_share_sums_to_100
  Purpose:
    Asserts that the sum of channel_revenue_share_pct across all channels
    for a given month equals 100% (within a floating-point tolerance of ±0.1).
    This validates the revenue share calculation in gold_channel_performance
    and ensures the CIO channel strategy dashboard shows a correct 100% view.
    Tolerance is applied because floating-point rounding across many channels
    may produce sums of 99.99 or 100.01.
  Failure condition: any month where the sum is outside [99.9, 100.1].
  ============================================================
*/

with monthly_share_totals as (

  select
    year_month,
    round(sum(channel_revenue_share_pct), 4) as total_share_pct

  from {{ ref('gold_channel_performance') }}
  group by year_month

)

select
  year_month,
  total_share_pct
from monthly_share_totals
where total_share_pct < 99.9
   or total_share_pct > 100.1
