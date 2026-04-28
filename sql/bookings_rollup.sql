-- ============================================================
-- bookings_rollup.sql
-- Monthly bookings analysis with running totals and cohort view
-- Patterns: CTEs, running totals, LAG, conditional aggregation
-- ============================================================

-- -----------------------------------------------
-- Query 1: Monthly bookings by rep and manager
-- with quota attainment and running total
-- -----------------------------------------------
WITH monthly_bookings AS (
    SELECT
        rep_id,
        rep_name,
        manager_name,
        region,
        YEAR(booking_date)  AS yr,
        MONTH(booking_date) AS mn,
        SUM(booking_value)  AS monthly_value,
        COUNT(*)            AS deal_count,
        AVG(booking_value)  AS avg_deal_size
    FROM revenue_bookings
    WHERE booking_status = 'Closed Won'
    GROUP BY rep_id, rep_name, manager_name, region,
             YEAR(booking_date), MONTH(booking_date)
)
SELECT
    rep_id,
    rep_name,
    manager_name,
    region,
    yr, mn,
    monthly_value,
    deal_count,
    ROUND(avg_deal_size, 2)                                              AS avg_deal_size,

    -- Running total per rep (cumulative bookings YTD)
    SUM(monthly_value) OVER (
        PARTITION BY rep_id, yr
        ORDER BY mn
        ROWS UNBOUNDED PRECEDING
    )                                                                    AS ytd_bookings,

    -- Monthly quota attainment (assuming quota stored separately)
    ROUND(monthly_value / NULLIF(q.monthly_quota, 0) * 100, 2)          AS quota_attainment_pct,

    -- MoM growth per rep
    ROUND(
        (monthly_value - LAG(monthly_value, 1) OVER (PARTITION BY rep_id ORDER BY yr, mn))
        / NULLIF(LAG(monthly_value, 1) OVER (PARTITION BY rep_id ORDER BY yr, mn), 0) * 100
    , 2)                                                                 AS mom_growth_pct,

    -- Performance band
    CASE
        WHEN ROUND(monthly_value / NULLIF(q.monthly_quota, 0) * 100, 2) >= 100 THEN 'On Target'
        WHEN ROUND(monthly_value / NULLIF(q.monthly_quota, 0) * 100, 2) >= 80  THEN 'At Risk'
        ELSE 'Below Target'
    END                                                                  AS attainment_band

FROM monthly_bookings mb
LEFT JOIN quota q ON mb.rep_id = q.rep_id
               AND mb.yr = q.quota_year
               AND mb.mn = q.quota_month
ORDER BY yr, mn, monthly_value DESC;


-- -----------------------------------------------
-- Query 2: Team-level bookings summary
-- with each rep's share of team total
-- Patterns: % of total using SUM() OVER (PARTITION BY)
-- -----------------------------------------------
WITH team_bookings AS (
    SELECT
        manager_name,
        rep_id,
        rep_name,
        SUM(booking_value) AS total_bookings
    FROM revenue_bookings
    WHERE booking_status = 'Closed Won'
    AND YEAR(booking_date) = YEAR(GETDATE())
    GROUP BY manager_name, rep_id, rep_name
)
SELECT
    manager_name,
    rep_name,
    total_bookings,

    -- Team total
    SUM(total_bookings) OVER (PARTITION BY manager_name)                AS team_total,

    -- Rep's % of team
    ROUND(total_bookings / NULLIF(SUM(total_bookings) OVER (PARTITION BY manager_name), 0) * 100, 2)
                                                                        AS pct_of_team,

    -- Rep's % of org
    ROUND(total_bookings / NULLIF(SUM(total_bookings) OVER (), 0) * 100, 2)
                                                                        AS pct_of_org,

    -- Rank within team
    RANK() OVER (PARTITION BY manager_name ORDER BY total_bookings DESC) AS team_rank

FROM team_bookings
ORDER BY manager_name, team_rank;


-- -----------------------------------------------
-- Query 3: Bookings cohort analysis
-- Group deals by the month they were first created
-- and track how long they took to close
-- Patterns: DATEDIFF, cohort grouping, conditional count
-- -----------------------------------------------
SELECT
    -- Cohort = month deal was created
    YEAR(created_date)                                                   AS cohort_yr,
    MONTH(created_date)                                                  AS cohort_mn,
    COUNT(*)                                                             AS deals_created,

    -- How many closed within 30/60/90 days
    SUM(CASE WHEN booking_status = 'Closed Won'
             AND DATEDIFF(DAY, created_date, booking_date) <= 30
             THEN 1 ELSE 0 END)                                          AS closed_30d,
    SUM(CASE WHEN booking_status = 'Closed Won'
             AND DATEDIFF(DAY, created_date, booking_date) <= 60
             THEN 1 ELSE 0 END)                                          AS closed_60d,
    SUM(CASE WHEN booking_status = 'Closed Won'
             AND DATEDIFF(DAY, created_date, booking_date) <= 90
             THEN 1 ELSE 0 END)                                          AS closed_90d,
    SUM(CASE WHEN booking_status = 'Closed Won'
             THEN 1 ELSE 0 END)                                          AS total_closed,

    -- Average days to close for won deals
    ROUND(AVG(CASE WHEN booking_status = 'Closed Won'
                   THEN DATEDIFF(DAY, created_date, booking_date)
              END), 1)                                                   AS avg_days_to_close,

    -- Win rate
    ROUND(SUM(CASE WHEN booking_status = 'Closed Won' THEN 1 ELSE 0 END)
          * 100.0 / NULLIF(COUNT(*), 0), 2)                             AS win_rate_pct

FROM revenue_bookings
GROUP BY YEAR(created_date), MONTH(created_date)
ORDER BY cohort_yr, cohort_mn;
