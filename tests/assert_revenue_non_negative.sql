/*
  ============================================================
  Singular Test: assert_revenue_non_negative
  Purpose:
    Asserts that no booked ticket record in silver_tickets has a negative
    face_value_usd. A negative revenue value indicates a data quality issue
    in the ingestion pipeline (e.g. sign inversion, corrupt currency rate).
    This test gates all gold revenue models.
  Failure condition: any row returned = test fails.
  ============================================================
*/

select
  ticket_id,
  face_value_usd,
  ticket_status,
  purchase_date
from {{ ref('silver_tickets') }}
where ticket_status = 'booked'
  and face_value_usd < 0
