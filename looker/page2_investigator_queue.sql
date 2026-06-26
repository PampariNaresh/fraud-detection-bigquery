-- ============================================================
-- PAGE 2 — Investigator Queue (Transaction Explorer)
-- Looker Studio Data Source: reports.high_risk_alerts
--                            reports.fraud_predictions
-- ============================================================

-- CHART 1: High-Risk Alert Table (main investigator view)
-- Chart name: "High-Risk Alert Queue"
-- Type: Table with conditional formatting
--   HIGH risk  → red background (#ea4335)
--   MEDIUM risk → orange background (#fa8c16)
-- Sort: fraud_probability DESC
SELECT
    DATE(alert_date)    AS alert_date,
    type,
    ROUND(amount, 2)    AS amount,
    nameOrig,
    nameDest,
    ROUND(fraud_probability, 4) AS fraud_probability,
    risk_level,
    top_reason,
    alert_status
FROM `fraud-detection-500305-500517.reports.high_risk_alerts`
ORDER BY fraud_probability DESC;

-- CHART 2: Alerts by Status
-- Chart name: "Alert Status Breakdown"
-- Type: Pie Chart
-- Dimension: alert_status   Metric: COUNT(*)
SELECT
    alert_status,
    COUNT(*) AS alert_count
FROM `fraud-detection-500305-500517.reports.high_risk_alerts`
GROUP BY alert_status
ORDER BY alert_count DESC;

-- CHART 3: Top Fraud Reasons
-- Chart name: "Top Fraud Trigger Reasons"
-- Type: Horizontal Bar Chart
-- Dimension: top_reason   Metric: COUNT(*)
SELECT
    top_reason,
    COUNT(*)                                          AS occurrence_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_alerts
FROM `fraud-detection-500305-500517.reports.high_risk_alerts`
GROUP BY top_reason
ORDER BY occurrence_count DESC
LIMIT 10;

-- CHART 4: High-Risk Amount by Transaction Type
-- Chart name: "Amount at Risk by Transaction Type"
-- Type: Bar Chart
-- Dimension: type   Metric: SUM(amount)
SELECT
    type,
    COUNT(*)                    AS alert_count,
    ROUND(SUM(amount), 2)       AS total_amount_at_risk,
    ROUND(AVG(amount), 2)       AS avg_alert_amount,
    ROUND(AVG(fraud_probability), 4) AS avg_fraud_score
FROM `fraud-detection-500305-500517.reports.high_risk_alerts`
GROUP BY type
ORDER BY total_amount_at_risk DESC;

-- CHART 5: Fraud Score Heatmap by Hour
-- Chart name: "Fraud Activity by Hour of Day"
-- Type: Bar Chart
-- Dimension: alert_hour   Metric: COUNT(*)
SELECT
    EXTRACT(HOUR FROM alert_date) AS alert_hour,
    COUNT(*)                       AS alert_count,
    ROUND(AVG(fraud_probability), 4) AS avg_fraud_probability,
    ROUND(SUM(amount), 2)           AS total_amount
FROM `fraud-detection-500305-500517.reports.high_risk_alerts`
GROUP BY alert_hour
ORDER BY alert_hour;

-- CHART 6: High-Risk Accounts (Top Originators)
-- Chart name: "Top High-Risk Origin Accounts"
-- Type: Table
SELECT
    nameOrig,
    COUNT(*)                         AS alert_count,
    ROUND(SUM(amount), 2)            AS total_amount_flagged,
    ROUND(MAX(fraud_probability), 4) AS max_fraud_score,
    ARRAY_AGG(DISTINCT type)         AS transaction_types
FROM `fraud-detection-500305-500517.reports.high_risk_alerts`
GROUP BY nameOrig
HAVING alert_count > 1
ORDER BY alert_count DESC
LIMIT 20;

-- LOOKER STUDIO FILTERS FOR THIS PAGE:
-- Filter 1: Date Range Control  → field: alert_date
-- Filter 2: Dropdown Filter     → field: risk_level   (HIGH / MEDIUM)
-- Filter 3: Dropdown Filter     → field: type         (TRANSFER / CASH_OUT / ...)
-- Filter 4: Dropdown Filter     → field: alert_status (OPEN / REVIEWED / CLOSED)
