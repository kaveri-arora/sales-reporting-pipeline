**Automated MIS & KPI Reporting for Sales Operations**

> Built during my role as Business Analyst at a global B2B events company. This repo demonstrates the methodology using synthetic data that mirrors the structure of the original pipeline.

---

## 🧩 Problem

A large sales operations team managed 6+ recurring reports across weekly and monthly cycles — rep activity trackers, bookings files, forecasting rollups, KPI dashboards, and commission registers. Each was built manually: pulling from multiple source systems, reformatting, reconciling, and distributing to leadership.

This created three compounding issues:
- **Reporting delays** during critical commercial periods (quarter-end, event cycles)
- **Data inconsistency** from manual copy-paste across sources
- **Analyst bottleneck** — report production consumed time that should go to analysis

---

## ✅ Solution

Designed and automated an end-to-end reporting pipeline consolidating all recurring MIS into a scheduled, multi-source data workflow.

### Pipeline Architecture

```
Source Systems
│
├── Sponsorship Export (weekly)
├── Rep Activity Data (weekly)
├── Revenue & Bookings (monthly)
└── Commission & Quota Data (monthly)
         │
         ▼
   [Alteryx Workflow]
   - Data ingestion & joins
   - Validation & reconciliation
   - Transformation & rollups
         │
         ▼
   Output Reports
   ├── Weekly Rep Activity Dashboard
   ├── Weekly Sales Progression Tracker
   ├── Monthly Bookings File
   ├── Monthly Forecasting File (CY/FB by rep & manager)
   └── Monthly Commission Register
         │
         ▼
   Leadership Distribution (automated)
```

---

## 🛠️ Tools & Stack

| Layer | Tool |
|---|---|
| Data pipeline & automation | Alteryx |
| Querying & transformation | SQL |
| Report formatting & output | Excel (advanced) |
| Visualisation | Power BI |
| Source data | CRM exports, sponsorship systems, revenue data |

---

## 📁 Repo Structure

```
sales-reporting-pipeline/
│
├── data/
│   ├── synthetic_rep_activity.csv        # Weekly rep KPI data (synthetic)
│   ├── synthetic_bookings.csv            # Monthly bookings by rep (synthetic)
│   ├── synthetic_sponsorship_export.csv  # Weekly sponsorship pipeline (synthetic)
│   └── synthetic_commission.csv         # Commission & quota tracker (synthetic)
│
├── sql/
│   ├── rep_activity_kpi_query.sql        # Weekly KPI aggregation logic
│   ├── bookings_rollup.sql               # Monthly bookings by rep & manager
│   ├── forecasting_rollup.sql            # CY/FB forecast reconciliation
│   └── pipeline_reconciliation.sql       # Multi-source data validation checks
│
├── alteryx/
│   └── workflow_screenshots/             # Screenshots of Alteryx workflow design
│
├── output/
│   ├── sample_rep_dashboard.png          # Sample KPI dashboard output
│   └── sample_bookings_summary.png       # Sample monthly bookings summary
│
└── README.md
```

---

## 🔍 Key SQL Logic

### Weekly Rep Activity KPI Rollup
```sql
SELECT
    rep_id,
    rep_name,
    manager_name,
    SUM(calls_made)        AS total_calls,
    SUM(meetings_booked)   AS total_meetings,
    SUM(proposals_sent)    AS total_proposals,
    SUM(deals_closed)      AS total_closed,
    ROUND(SUM(deals_closed) * 100.0 / NULLIF(SUM(calls_made), 0), 2) AS call_to_close_pct,
    week_start_date
FROM rep_activity
WHERE week_start_date >= DATEADD(week, -4, GETDATE())
GROUP BY rep_id, rep_name, manager_name, week_start_date
ORDER BY week_start_date DESC, total_closed DESC;
```

### Month-over-Month Forecasting Reconciliation
```sql
SELECT
    rep_id,
    rep_name,
    forecast_month,
    cy_forecast,
    fb_forecast,
    actual_bookings,
    ROUND((actual_bookings - cy_forecast) * 100.0 / NULLIF(cy_forecast, 0), 2) AS cy_variance_pct,
    ROUND((actual_bookings - fb_forecast) * 100.0 / NULLIF(fb_forecast, 0), 2) AS fb_variance_pct
FROM forecasting_data
ORDER BY forecast_month DESC, rep_name;
```

### Multi-Source Data Validation
```sql
-- Identify mismatches between revenue system and sponsorship export
SELECT
    s.account_id,
    s.account_name,
    s.sponsorship_revenue  AS revenue_in_sponsorship_export,
    r.booked_revenue       AS revenue_in_bookings_system,
    ABS(s.sponsorship_revenue - r.booked_revenue) AS discrepancy
FROM sponsorship_export s
LEFT JOIN revenue_bookings r ON s.account_id = r.account_id
WHERE ABS(s.sponsorship_revenue - r.booked_revenue) > 500
ORDER BY discrepancy DESC;
```

---

## 📈 Results

| Metric | Outcome |
|---|---|
| Manual reporting effort | ~30% reduction across weekly and monthly cycles |
| Reports automated | 6+ recurring MIS across 3 business units |
| Stakeholders served | Sales reps, managers, and senior leadership |
| Data sources integrated | Revenue, inventory, activity, sponsorship spend |
| Reporting cadence | Weekly and monthly automated refreshes |

---

## 💡 Key Design Decisions

**Why Alteryx over pure SQL?**
The pipeline needed to pull from multiple source systems with different formats and schedules. Alteryx allowed visual workflow design, easy scheduling, and non-technical stakeholder handoff — which mattered for sustainability when I'm not available.

**Why not a BI tool alone?**
Power BI was used for the dashboard layer, but the underlying data needed cleaning, joining, and validation logic that lived upstream. The pipeline handles transformation; BI handles presentation.

**Data validation as a pipeline step**
Rather than catching errors post-distribution, reconciliation checks were built into the pipeline itself — flagging mismatches between source systems before reports were sent to leadership.

---

## ⚠️ Note on Data

All data in this repository is **synthetic** and generated to mirror the structure of real operational data. No proprietary or confidential information is included. Column names and structures reflect real-world sales operations reporting patterns.

---

*Part of my analytics portfolio — linkedin.com/in/kaveri-arora-2a702313b/| github.com/kaveri-arora*
