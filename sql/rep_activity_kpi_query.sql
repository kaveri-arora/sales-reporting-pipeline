-- ============================================================
-- rep_activity_kpi_query.sql
-- Weekly rep activity KPI aggregation with performance ranking
-- Patterns: GROUP BY, conditional aggregation, RANK() window function
-- ============================================================

-- -----------------------------------------------
-- Query 1: Weekly KPI rollup per rep (last 4 weeks)
-- -----------------------------------------------
SELECT
    rep_id,
    rep_name,
    manager_name,
    week_start_date,
    SUM(calls_made)                                                        AS total_calls,
    SUM(meetings_booked)                                                   AS total_meetings,
    SUM(proposals_sent)                                                    AS total_proposals,
    SUM(deals_closed)                                                      AS total_closed,
    SUM(booking_value)                                                     AS total_booking_value,

    -- Conversion rates
    ROUND(SUM(meetings_booked) * 100.0 / NULLIF(SUM(calls_made), 0), 2)  AS call_to_meeting_pct,
    ROUND(SUM(deals_closed)    * 100.0 / NULLIF(SUM(calls_made), 0), 2)  AS call_to_close_pct,
    ROUND(SUM(deals_closed)    * 100.0 / NULLIF(SUM(proposals_sent), 0), 2) AS proposal_to_close_pct,

    -- Performance band
    CASE
        WHEN ROUND(SUM(deals_closed) * 100.0 / NULLIF(SUM(calls_made), 0), 2) >= 15 THEN 'High Performer'
        WHEN ROUND(SUM(deals_closed) * 100.0 / NULLIF(SUM(calls_made), 0), 2) >= 8  THEN 'On Track'
        ELSE 'Needs Support'
    END AS performance_band

FROM rep_activity
WHERE week_start_date >= DATEADD(WEEK, -4, GETDATE())
GROUP BY rep_id, rep_name, manager_name, week_start_date
ORDER BY week_start_date DESC, total_closed DESC;


-- -----------------------------------------------
-- Query 2: Rep ranking within each manager team
-- Using RANK() window function — top performers per team
-- -----------------------------------------------
WITH weekly_totals AS (
    SELECT
        rep_id,
        rep_name,
        manager_name,
        SUM(deals_closed)    AS total_closed,
        SUM(booking_value)   AS total_value,
        ROUND(SUM(deals_closed) * 100.0 / NULLIF(SUM(calls_made), 0), 2) AS close_rate
    FROM rep_activity
    WHERE week_start_date >= DATEADD(WEEK, -4, GETDATE())
    GROUP BY rep_id, rep_name, manager_name
)
SELECT
    rep_id,
    rep_name,
    manager_name,
    total_closed,
    total_value,
    close_rate,
    -- Rank within each manager's team
    RANK() OVER (PARTITION BY manager_name ORDER BY total_value DESC)  AS team_rank,
    -- Rank across entire sales org
    RANK() OVER (ORDER BY total_value DESC)                            AS org_rank,
    -- Each rep's share of their team's total bookings
    ROUND(total_value / NULLIF(SUM(total_value) OVER (PARTITION BY manager_name), 0) * 100, 2) AS pct_of_team_bookings
FROM weekly_totals
ORDER BY manager_name, team_rank;


-- -----------------------------------------------
-- Query 3: Week-over-week activity trend per rep
-- Using LAG() to compare this week vs last week
-- -----------------------------------------------
WITH weekly_activity AS (
    SELECT
        rep_id,
        rep_name,
        week_start_date,
        SUM(calls_made)    AS calls,
        SUM(deals_closed)  AS closed,
        SUM(booking_value) AS value
    FROM rep_activity
    GROUP BY rep_id, rep_name, week_start_date
)
SELECT
    rep_id,
    rep_name,
    week_start_date,
    calls,
    closed,
    value,

    -- Previous week values using LAG
    LAG(calls,  1) OVER (PARTITION BY rep_id ORDER BY week_start_date)  AS prev_calls,
    LAG(closed, 1) OVER (PARTITION BY rep_id ORDER BY week_start_date)  AS prev_closed,
    LAG(value,  1) OVER (PARTITION BY rep_id ORDER BY week_start_date)  AS prev_value,

    -- Week-over-week change
    calls  - LAG(calls,  1) OVER (PARTITION BY rep_id ORDER BY week_start_date) AS calls_wow_delta,
    closed - LAG(closed, 1) OVER (PARTITION BY rep_id ORDER BY week_start_date) AS closed_wow_delta,

    -- WoW growth %
    ROUND(
        (value - LAG(value, 1) OVER (PARTITION BY rep_id ORDER BY week_start_date))
        / NULLIF(LAG(value, 1) OVER (PARTITION BY rep_id ORDER BY week_start_date), 0) * 100
    , 2) AS value_wow_growth_pct,

    -- Flag reps whose activity dropped significantly
    CASE
        WHEN calls < LAG(calls, 1) OVER (PARTITION BY rep_id ORDER BY week_start_date) * 0.7
        THEN 'Activity Drop - Review'
        ELSE 'Normal'
    END AS activity_flag

FROM weekly_activity
ORDER BY rep_id, week_start_date DESC;
