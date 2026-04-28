-- ============================================================
-- pipeline_reconciliation.sql
-- Multi-source data validation and anomaly detection
-- Patterns: LEFT JOIN, IS NULL, CASE WHEN, statistical checks
--
-- Business context: At Gartner I identified ~200 records being
-- silently excluded each cycle due to INNER JOIN logic on
-- deprecated account IDs. This file documents the validation
-- framework built to catch silent data drops before they reach
-- stakeholders. Same pattern applies to subscription analytics —
-- detecting tracking failures before they surface as fake metric drops.
-- ============================================================

-- -----------------------------------------------
-- Query 1: Cross-source reconciliation
-- Identify mismatches between revenue system and sponsorship export
-- Patterns: LEFT JOIN, ABS() for variance, CASE WHEN banding
-- -----------------------------------------------
SELECT
    COALESCE(s.account_id, r.account_id)                                AS account_id,
    COALESCE(s.account_name, r.account_name)                           AS account_name,
    s.sponsorship_revenue                                               AS revenue_in_sponsorship,
    r.booked_revenue                                                    AS revenue_in_bookings,
    ABS(COALESCE(s.sponsorship_revenue, 0)
        - COALESCE(r.booked_revenue, 0))                               AS discrepancy_amount,
    ROUND(
        ABS(COALESCE(s.sponsorship_revenue, 0)
            - COALESCE(r.booked_revenue, 0))
        / NULLIF(COALESCE(r.booked_revenue, s.sponsorship_revenue), 0) * 100
    , 2)                                                               AS discrepancy_pct,

    -- Classify the type of discrepancy
    CASE
        WHEN s.account_id IS NULL THEN 'Missing in sponsorship export'
        WHEN r.account_id IS NULL THEN 'Missing in bookings system'
        WHEN ABS(s.sponsorship_revenue - r.booked_revenue) > 10000 THEN 'Large variance - investigate'
        WHEN ABS(s.sponsorship_revenue - r.booked_revenue) > 500   THEN 'Minor variance - review'
        ELSE 'Reconciled'
    END                                                                AS reconciliation_status

FROM sponsorship_export s
FULL OUTER JOIN revenue_bookings r ON s.account_id = r.account_id
WHERE
    s.account_id IS NULL                                  -- in bookings but not sponsorship
    OR r.account_id IS NULL                               -- in sponsorship but not bookings
    OR ABS(s.sponsorship_revenue - r.booked_revenue) > 500 -- meaningful variance
ORDER BY discrepancy_amount DESC;


-- -----------------------------------------------
-- Query 2: Row count assertion across pipeline stages
-- Catches silent data drops at each transformation step
-- Patterns: UNION ALL, LAG, percentage change calculation
-- -----------------------------------------------
WITH stage_counts AS (
    SELECT 'raw_extract'     AS stage, 1 AS stage_order, COUNT(*) AS row_count FROM raw_bookings_extract
    UNION ALL
    SELECT 'after_join',               2,                 COUNT(*) FROM bookings_after_join
    UNION ALL
    SELECT 'after_validation',         3,                 COUNT(*) FROM bookings_validated
    UNION ALL
    SELECT 'final_output',             4,                 COUNT(*) FROM bookings_final
),
with_prev AS (
    SELECT
        stage,
        stage_order,
        row_count,
        LAG(row_count, 1) OVER (ORDER BY stage_order) AS prev_count
    FROM stage_counts
)
SELECT
    stage,
    row_count,
    prev_count,
    row_count - prev_count                                             AS rows_delta,
    ROUND(
        (row_count - prev_count) * 100.0 / NULLIF(prev_count, 0)
    , 2)                                                               AS delta_pct,

    -- Validation status
    CASE
        WHEN prev_count IS NULL                                        THEN 'Source stage'
        WHEN ABS(row_count - prev_count) * 100.0
             / NULLIF(prev_count, 0) > 5                              THEN 'ALERT: >5% row change - investigate'
        WHEN ABS(row_count - prev_count) * 100.0
             / NULLIF(prev_count, 0) > 1                              THEN 'WARNING: >1% row change - review'
        ELSE                                                                'OK'
    END                                                                AS validation_status

FROM with_prev
ORDER BY stage_order;


-- -----------------------------------------------
-- Query 3: Detect stale or dormant accounts in pipeline
-- Accounts with no activity in 30+ days — at churn risk
-- Patterns: MAX for last event, DATEDIFF, LEFT ANTI JOIN
-- -----------------------------------------------
WITH last_activity AS (
    SELECT
        account_id,
        MAX(activity_date)                                             AS last_activity_date,
        COUNT(*)                                                       AS total_activities,
        SUM(CASE WHEN activity_type = 'meeting' THEN 1 ELSE 0 END)    AS meetings_held,
        SUM(CASE WHEN activity_type = 'proposal_sent' THEN 1 ELSE 0 END) AS proposals_sent
    FROM rep_activity
    GROUP BY account_id
),
open_pipeline AS (
    SELECT account_id, deal_id, booking_value, stage, created_date
    FROM revenue_bookings
    WHERE booking_status NOT IN ('Closed Won', 'Closed Lost')
)
SELECT
    o.account_id,
    o.deal_id,
    o.booking_value,
    o.stage,
    o.created_date,
    la.last_activity_date,
    DATEDIFF(DAY, la.last_activity_date, GETDATE())                    AS days_since_activity,
    la.meetings_held,
    la.proposals_sent,

    -- Risk classification
    CASE
        WHEN la.account_id IS NULL
             THEN 'No activity recorded - at risk'
        WHEN DATEDIFF(DAY, la.last_activity_date, GETDATE()) > 60
             THEN 'Stale >60 days - likely lost'
        WHEN DATEDIFF(DAY, la.last_activity_date, GETDATE()) > 30
             THEN 'Stale 30-60 days - needs follow-up'
        ELSE 'Active'
    END                                                                AS pipeline_health

FROM open_pipeline o
LEFT JOIN last_activity la ON o.account_id = la.account_id
WHERE la.account_id IS NULL
   OR DATEDIFF(DAY, la.last_activity_date, GETDATE()) > 30
ORDER BY days_since_activity DESC;


-- -----------------------------------------------
-- Query 4: Forecasting rollup with CY and FB variance
-- Patterns: ROUND, NULLIF, variance calculation
-- -----------------------------------------------
SELECT
    rep_id,
    rep_name,
    manager_name,
    forecast_month,
    cy_forecast,
    fb_forecast,
    actual_bookings,

    -- Variance from CY forecast
    actual_bookings - cy_forecast                                      AS cy_variance_abs,
    ROUND((actual_bookings - cy_forecast) * 100.0
          / NULLIF(cy_forecast, 0), 2)                                AS cy_variance_pct,

    -- Variance from FB forecast
    actual_bookings - fb_forecast                                      AS fb_variance_abs,
    ROUND((actual_bookings - fb_forecast) * 100.0
          / NULLIF(fb_forecast, 0), 2)                                AS fb_variance_pct,

    -- Forecast accuracy band
    CASE
        WHEN ABS(actual_bookings - cy_forecast) * 100.0
             / NULLIF(cy_forecast, 0) <= 5  THEN 'Accurate (<5% variance)'
        WHEN ABS(actual_bookings - cy_forecast) * 100.0
             / NULLIF(cy_forecast, 0) <= 15 THEN 'Acceptable (5-15% variance)'
        ELSE                                     'Inaccurate (>15% variance) - review forecast methodology'
    END                                                                AS forecast_accuracy

FROM forecasting_data
ORDER BY forecast_month DESC, ABS(cy_variance_pct) DESC;
