# Alteryx Pipeline — Architecture & Tool Documentation

## Why the workflow file is not included

The Alteryx workflow (`.yxmd`) connects to live source systems and contains
scheduled connection credentials. Sharing it publicly is not appropriate.
This README documents the full pipeline architecture, tools used, and design
decisions in place of the workflow file.

---

## Pipeline Overview

The workflow runs in **Alteryx Designer** (development) and **Alteryx Server**
(scheduled execution). It ingests from 4 source systems, applies transformation
and validation logic, and outputs 5 formatted reports.

```
[Source Systems]          [Alteryx Workflow]        [Outputs]
─────────────────         ──────────────────        ─────────
Sponsorship Export   →    Ingest + Validate    →    Weekly Rep Activity
Rep Activity Data    →    Join + Reconcile     →    Weekly Sales Tracker
Revenue & Bookings   →    Transform + Rollup   →    Monthly Bookings
Commission/Quota     →    Validate + Output    →    Monthly Forecast
                                                    Monthly Commission
```

---

## Tool-by-Tool Workflow

### 1. Input Data (×4)
Ingests each source file on its schedule:
- `sponsorship_export.csv` — weekly
- `rep_activity.csv` — weekly
- `revenue_bookings.csv` — monthly
- `commission_quota.csv` — monthly

**Configuration:** File path, delimiter, header row detection, data type inference

---

### 2. Select
Applied after each Input tool.
- Keeps only required columns
- Renames columns to standard naming convention (`Opp_Amt` → `amount`)
- Enforces correct data types (dates, decimals, integers)

---

### 3. Data Cleansing
One-click standardisation:
- Removes leading/trailing whitespace from rep and account names
- Fills null numeric fields with 0
- Standardises blank strings to null

---

### 4. Filter
Removes invalid records before any transformation:

**Condition:** `[record_status] != "Test" AND [record_status] != "Deleted"`

- TRUE stream → continues through pipeline
- FALSE stream → routed to exception log for audit

---

### 5. Join (×2)
**Join 1:** Rep activity + quota data on `rep_id`
- Output: one row per rep with both activity metrics and quota targets

**Join 2:** Sponsorship export + revenue bookings on `account_id`
- Output: reconciled revenue view for cross-source validation

**Design decision:** LEFT JOIN used throughout (not INNER JOIN) to surface
unmatched records rather than silently excluding them.

---

### 6. Formula
Adds calculated columns:

```
attainment_pct     = [actual_bookings] / [cy_forecast]
call_to_close_pct  = [deals_closed] / NULLIF([calls_made], 0)
cy_variance        = [actual_bookings] - [cy_forecast]
cy_variance_pct    = ([actual_bookings] - [cy_forecast]) / NULLIF([cy_forecast], 0)
deal_age_days      = DateTimeDiff([close_date], [create_date], "days")
```

---

### 7. Multi-Row Formula
Calculates row-to-row comparisons (equivalent to SQL LAG function):

```
mom_revenue_change    = [amount] - [Row-1:amount]
running_ytd_bookings  = [Row-1:running_ytd_bookings] + [amount]
```

Sorted by `rep_id ASC, forecast_month ASC` before this step.

---

### 8. Summarize
Aggregates for leadership rollup reports:

| Group By | Aggregations |
|----------|-------------|
| rep_id, rep_name, manager_name, week_start_date | SUM(calls_made), SUM(meetings_booked), SUM(deals_closed), SUM(revenue) |
| manager_name, forecast_month | SUM(cy_forecast), SUM(actual_bookings), AVG(attainment_pct) |
| region, quarter | SUM(revenue), COUNT(deals), MAX(deal_size) |

---

### 9. Validation Checks (embedded at every join)

**Row count assertion after each transformation:**
```
IF RowCount < [expected_minimum] THEN route to ALERT
```

**Cross-source reconciliation (sponsorship vs bookings):**
```
WHERE ABS(sponsorship_revenue - booked_revenue) > 500
→ Routes discrepancies to exception report
→ Pipeline continues with flagged records
→ Leadership receives exception log alongside main report
```

**Key design decision:** Errors surface as flagged exceptions, not silent exclusions.
This was the fix for the deficiency where INNER JOINs silently dropped ~200 records
per cycle in a previous version of the pipeline.

---

### 10. Output Data (×5)
Each report output to a separate Excel file with:
- Overwrite mode (not append) for weekly/monthly reports
- Tab name matching report name
- Header row included
- Formatted as Excel Table for Power BI connection

**Reports output:**
1. `weekly_rep_activity_dashboard.xlsx`
2. `weekly_sales_progression_tracker.xlsx`
3. `monthly_bookings_file.xlsx`
4. `monthly_forecasting_file.xlsx`
5. `monthly_commission_register.xlsx`

---

## Validation Framework

Four-dimension validation embedded in pipeline:

| Dimension | Check | Action if Fails |
|-----------|-------|-----------------|
| Completeness | Row count vs expected baseline | Alert + hold |
| Consistency | Cross-source revenue reconciliation | Flag + continue |
| Accuracy | Range checks on key numeric fields | Flag + continue |
| Timeliness | Source file arrival within SLA window | Alert + hold |

---

## Scheduling

- **Weekly reports:** Alteryx Server scheduled Monday 6AM
- **Monthly reports:** First working day of month, 6AM
- **Distribution:** Automated email via Alteryx output configuration

---

## Results

| Metric | Before | After |
|--------|--------|-------|
| Manual reporting effort | ~8 hrs/week | ~45 mins/week |
| Processing time reduction | — | 85% |
| Monthly analyst hours freed | — | 200+ |
| Data error rate | Baseline | -60% |
| On-time delivery | ~80% | 95% |

---

*Part of analytics portfolio — [linkedin.com/in/kaveri-arora-2a702313b](https://linkedin.com/in/kaveri-arora-2a702313b) | [github.com/kaveri-arora](https://github.com/kaveri-arora)*
