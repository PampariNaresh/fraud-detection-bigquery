# Fraud Detection — Looker Studio Dashboard Spec
**Project:** fraud-detection-500305-500517
**File:** dashboard_spec.md

---

## Data Sources (connect these first in Looker Studio)

| Source Name        | BigQuery Table                                    |
|--------------------|---------------------------------------------------|
| fraud_predictions  | fraud-detection-500305-500517.reports.fraud_predictions  |
| high_risk_alerts   | fraud-detection-500305-500517.reports.high_risk_alerts   |
| daily_summary      | fraud-detection-500305-500517.reports.daily_summary      |
| eval_log           | fraud-detection-500305-500517.ml_models.model_evaluation_log |
| training_log       | fraud-detection-500305-500517.ml_models.training_log     |
| feature_table      | fraud-detection-500305-500517.features.transaction_features  |

---

## Page 1 — Executive Summary

**SQL file:** `page1_executive_summary.sql`

| # | Chart Name                              | Type            | Source         | Dimension            | Metric                         |
|---|-----------------------------------------|-----------------|----------------|----------------------|--------------------------------|
| 1 | Total Transactions                      | Scorecard       | daily_summary  | —                    | SUM(total_transactions)        |
| 2 | High Risk Alerts Today                  | Scorecard (red) | daily_summary  | —                    | SUM(high_risk_count)           |
| 3 | Total Amount at Risk ($)                | Scorecard (orange) | daily_summary | —                 | SUM(total_amount_at_risk)      |
| 4 | Avg Fraud Score                         | Scorecard       | daily_summary  | —                    | AVG(avg_fraud_probability)     |
| 5 | Daily High-Risk Transaction Trend       | Line Chart      | daily_summary  | summary_date         | high_risk_count, medium_risk_count |
| 6 | Risk Distribution (% of Transactions)  | Donut Chart     | fraud_predictions | risk_level        | COUNT(*)                       |
| 7 | Fraud Transactions by Type             | Bar Chart       | fraud_predictions | type              | actual_fraud_count, fraud_rate_pct |

**Filters:** Date Range Control → summary_date

---

## Page 2 — Investigator Queue

**SQL file:** `page2_investigator_queue.sql`

| # | Chart Name                          | Type            | Source           | Dimension      | Metric                          |
|---|-------------------------------------|-----------------|------------------|----------------|---------------------------------|
| 1 | High-Risk Alert Queue               | Table           | high_risk_alerts | alert_date, type, nameOrig, nameDest | fraud_probability, amount, risk_level, top_reason, alert_status |
| 2 | Alert Status Breakdown              | Pie Chart       | high_risk_alerts | alert_status   | COUNT(*)                        |
| 3 | Top Fraud Trigger Reasons           | Horizontal Bar  | high_risk_alerts | top_reason     | COUNT(*), pct_of_alerts         |
| 4 | Amount at Risk by Transaction Type  | Bar Chart       | high_risk_alerts | type           | SUM(amount), AVG(fraud_probability) |
| 5 | Fraud Activity by Hour of Day       | Bar Chart       | high_risk_alerts | HOUR(alert_date) | COUNT(*)                      |
| 6 | Top High-Risk Origin Accounts       | Table           | high_risk_alerts | nameOrig       | alert_count, total_amount_flagged, max_fraud_score |

**Filters:**
- Date Range Control → alert_date
- Dropdown → risk_level (HIGH / MEDIUM)
- Dropdown → type (TRANSFER / CASH_OUT / PAYMENT / ...)
- Dropdown → alert_status (OPEN / REVIEWED / CLOSED)

**Conditional formatting on Chart 1 (Alert Queue table):**
- risk_level = HIGH   → background #ea4335 (red)
- risk_level = MEDIUM → background #fa8c16 (orange)

---

## Page 3 — Model Health & Feature Insights

**SQL file:** `page3_model_health.sql`

| # | Chart Name                             | Type         | Source       | Dimension            | Metric                                   |
|---|----------------------------------------|--------------|--------------|----------------------|------------------------------------------|
| 1 | Model F1 Score (Latest)               | Scorecard    | eval_log     | —                    | f1_score (latest run)                    |
| 2 | Precision (Latest)                    | Scorecard    | eval_log     | —                    | precision (latest run)                   |
| 3 | Recall (Latest)                       | Scorecard    | eval_log     | —                    | recall (latest run)                      |
| 4 | AUC-ROC Score (Latest)                | Scorecard    | eval_log     | —                    | roc_auc (latest run)                     |
| 5 | Model Performance Over Time           | Line Chart   | eval_log     | run_date             | f1_score, roc_auc, precision, recall     |
| 6 | Model Comparison (All Runs)           | Table        | eval_log     | model_name, run_date | f1_score, precision, recall, roc_auc, log_loss |
| 7 | Training vs Evaluation Row Counts     | Grouped Bar  | training_log | model_name, run_date | training_rows, eval_rows                 |
| 8 | Prediction Score Distribution         | Bar (histo)  | fraud_predictions | prob_bucket*   | COUNT(*), fraud_hit_rate_pct             |
| 9 | Fraud Rate by Type & Hour             | Table/Heat   | feature_table | type, time_of_day  | fraud_count, fraud_rate_pct              |
| 10| Transaction Amount vs Fraud Score     | Scatter Chart | fraud_predictions | amount (X)   | fraud_probability (Y), color=risk_level  |

**Calculated field for Chart 8 (prob_bucket) — add in Looker Studio:**
```
CASE
  WHEN fraud_probability < 0.2 THEN "0.0 – 0.2"
  WHEN fraud_probability < 0.4 THEN "0.2 – 0.4"
  WHEN fraud_probability < 0.6 THEN "0.4 – 0.6"
  WHEN fraud_probability < 0.8 THEN "0.6 – 0.8"
  ELSE "0.8 – 1.0"
END
```

---

## Theme Settings

| Setting       | Value            |
|---------------|------------------|
| Theme         | Simple Dark      |
| Primary color | #1a73e8 (blue)   |
| Alert color   | #ea4335 (red)    |
| Warning color | #fa8c16 (orange) |
| Auto-refresh  | Every 15 minutes |
| Share         | View-only link   |
