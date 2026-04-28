-- ============================================================
-- advanced_analytics.sql
-- Advanced SQL patterns for sales operations analytics
-- Patterns: consecutive conditions, self join, DENSE_RANK,
--           running totals, all-conditions-met HAVING
--
-- These patterns directly mirror subscription analytics use cases:
--   consecutive declining calls → consecutive missed EMIs / streak breaks
--   commission register → MRR calculation by plan type
--   reps active last quarter not this → reactivated users pattern
-- ============================================================

-- -----------------------------------------------
-- Query 1: Reps with 3+ consecutive weeks of declining activity
-- Patterns: LAG for consecutive conditions, DATE_SUB
-- Subscription equivalent: users with declining lesson completion
-- -----------------------------------------------
WITH weekly_calls AS (
    -- Step 1: one row per rep per week
    SELECT
        rep_id,
        rep_name,
        week_start_date,
        SUM(calls_made) AS total_calls
    FROM rep_activity
    GROUP BY rep_id, rep_name, week_start_date
),
with_lag AS (
    -- Step 2: add previous 2 weeks using LAG
    SELECT
        rep_id,
        rep_name,
        week_start_date,
        total_calls,
        LAG(total_calls, 1) OVER (PARTITION BY rep_id ORDER BY week_start_date) AS prev1,
        LAG(total_calls, 2) OVER (PARTITION BY rep_id ORDER BY week_start_date) AS prev2
    FROM weekly_calls
)
-- Step 3: filter where all three weeks show decline
SELECT DISTINCT rep_id, rep_name,
    'Activity declining 3+ consecutive weeks' AS flag
FROM with_lag
WHERE total_calls < prev1
  AND prev1 < prev2
ORDER BY rep_id;


-- -----------------------------------------------
-- Query 2: Reps active last quarter but NOT this quarter
-- Patterns: LEFT ANTI JOIN, quarter-over-quarter comparison
-- Subscription equivalent: reactivated users (inactive last month, active this month)
-- -----------------------------------------------
WITH q_prev AS (
    SELECT DISTINCT rep_id
    FROM revenue_bookings
    WHERE booking_status = 'Closed Won'
    AND DATEPART(QUARTER, booking_date) = DATEPART(QUARTER, DATEADD(QUARTER, -1, GETDATE()))
    AND YEAR(booking_date) = YEAR(DATEADD(QUARTER, -1, GETDATE()))
),
q_curr AS (
    SELECT DISTINCT rep_id
    FROM revenue_bookings
    WHERE booking_status = 'Closed Won'
    AND DATEPART(QUARTER, booking_date) = DATEPART(QUARTER, GETDATE())
    AND YEAR(booking_date) = YEAR(GETDATE())
)
SELECT
    p.rep_id,
    r.rep_name,
    r.manager_name,
    'Closed last quarter but not this quarter - performance review needed' AS status
FROM q_prev p
JOIN reps r ON p.rep_id = r.rep_id
LEFT JOIN q_curr c ON p.rep_id = c.rep_id
WHERE c.rep_id IS NULL   -- LEFT ANTI JOIN: active last Q, gone quiet this Q
ORDER BY r.manager_name;


-- -----------------------------------------------
-- Query 3: Commission register with running total and rank
-- Patterns: DENSE_RANK, running total, % of total
-- Subscription equivalent: MRR by plan type with cumulative total
-- -----------------------------------------------
WITH commission_calc AS (
    SELECT
        rep_id,
        rep_name,
        manager_name,
        YEAR(booking_date)   AS yr,
        MONTH(booking_date)  AS mn,
        SUM(booking_value)   AS monthly_bookings,
        -- Tiered commission rate
        CASE
            WHEN SUM(booking_value) >= 100000 THEN SUM(booking_value) * 0.12
            WHEN SUM(booking_value) >= 50000  THEN SUM(booking_value) * 0.09
            WHEN SUM(booking_value) >= 20000  THEN SUM(booking_value) * 0.06
            ELSE                                   SUM(booking_value) * 0.04
        END AS commission_earned
    FROM revenue_bookings
    WHERE booking_status = 'Closed Won'
    GROUP BY rep_id, rep_name, manager_name,
             YEAR(booking_date), MONTH(booking_date)
)
SELECT
    rep_id, rep_name, manager_name, yr, mn,
    ROUND(monthly_bookings, 2)                                         AS monthly_bookings,
    ROUND(commission_earned, 2)                                        AS commission_earned,

    -- Cumulative commission YTD per rep
    ROUND(SUM(commission_earned) OVER (
        PARTITION BY rep_id, yr
        ORDER BY mn
        ROWS UNBOUNDED PRECEDING
    ), 2)                                                              AS ytd_commission,

    -- Rep's % of org total commission this month
    ROUND(commission_earned / NULLIF(
        SUM(commission_earned) OVER (PARTITION BY yr, mn)
    , 0) * 100, 2)                                                     AS pct_of_monthly_commission,

    -- Commission rank within team this month
    DENSE_RANK() OVER (
        PARTITION BY manager_name, yr, mn
        ORDER BY commission_earned DESC
    )                                                                  AS team_commission_rank

FROM commission_calc
ORDER BY yr, mn, commission_earned DESC;


-- -----------------------------------------------
-- Query 4: Reps who hit target every month of the quarter
-- Patterns: HAVING COUNT(DISTINCT ...) = N
-- Subscription equivalent: users active all 12 months of the year
-- -----------------------------------------------
WITH monthly_attainment AS (
    SELECT
        b.rep_id,
        r.rep_name,
        r.manager_name,
        YEAR(b.booking_date)  AS yr,
        MONTH(b.booking_date) AS mn,
        SUM(b.booking_value)  AS actual,
        q.monthly_quota       AS quota,
        CASE WHEN SUM(b.booking_value) >= q.monthly_quota
             THEN 1 ELSE 0 END AS hit_target
    FROM revenue_bookings b
    JOIN reps r ON b.rep_id = r.rep_id
    JOIN quota q ON b.rep_id = q.rep_id
                AND YEAR(b.booking_date)  = q.quota_year
                AND MONTH(b.booking_date) = q.quota_month
    WHERE booking_status = 'Closed Won'
    AND YEAR(b.booking_date) = YEAR(GETDATE())
    GROUP BY b.rep_id, r.rep_name, r.manager_name,
             YEAR(b.booking_date), MONTH(b.booking_date),
             q.monthly_quota
)
SELECT
    rep_id,
    rep_name,
    manager_name,
    COUNT(DISTINCT mn)                                                 AS months_with_data,
    SUM(hit_target)                                                    AS months_on_target,
    ROUND(AVG(actual * 1.0 / NULLIF(quota, 0)) * 100, 1)             AS avg_attainment_pct,
    'Consistent performer - hit target every month'                    AS recognition_flag
FROM monthly_attainment
GROUP BY rep_id, rep_name, manager_name
HAVING SUM(hit_target) = COUNT(DISTINCT mn)  -- hit target in ALL months
   AND COUNT(DISTINCT mn) >= 3               -- at least 3 months of data
ORDER BY avg_attainment_pct DESC;
