-- ============================================================
-- PAGE 1 — Executive Summary
-- Looker Studio Data Source: reports.daily_summary
--                            reports.fraud_predictions
-- ============================================================

-- CHART 1: KPI Scorecard — Total Transactions Today
-- Chart name: "Total Transactions"
-- Type: Scorecard
SELECT
    SUM(total_transactions) AS total_transactions
FROM `fraud-detection-500305-500517.reports.daily_summary`
WHERE DATE(summary_date) = CURRENT_DATE();

-- CHART 2: KPI Scorecard — High Risk Count
-- Chart name: "High Risk Alerts Today"
-- Type: Scorecard (red theme)
SELECT
    SUM(high_risk_count) AS high_risk_count
FROM `fraud-detection-500305-500517.reports.daily_summary`
WHERE DATE(summary_date) = CURRENT_DATE();

-- CHART 3: KPI Scorecard — Total Amount at Risk
-- Chart name: "Total Amount at Risk ($)"
-- Type: Scorecard (orange theme)
SELECT
    ROUND(SUM(total_amount_at_risk), 2) AS total_amount_at_risk
FROM `fraud-detection-500305-500517.reports.daily_summary`
WHERE DATE(summary_date) = CURRENT_DATE();

-- CHART 4: KPI Scorecard — Average Fraud Probability
-- Chart name: "Avg Fraud Score"
-- Type: Scorecard
SELECT
    ROUND(AVG(avg_fraud_probability), 4) AS avg_fraud_probability
FROM `fraud-detection-500305-500517.reports.daily_summary`
WHERE DATE(summary_date) = CURRENT_DATE();

-- CHART 5: Daily Fraud Trend
-- Chart name: "Daily High-Risk Transaction Trend"
-- Type: Time Series / Line Chart
-- Dimension: summary_date   Metric: high_risk_count, medium_risk_count
SELECT
    DATE(summary_date)   AS summary_date,
    SUM(high_risk_count)   AS high_risk_count,
    SUM(medium_risk_count) AS medium_risk_count,
    SUM(low_risk_count)    AS low_risk_count
FROM `fraud-detection-500305-500517.reports.daily_summary`
GROUP BY 1
ORDER BY 1 DESC;

-- CHART 6: Risk Level Distribution
-- Chart name: "Risk Distribution (% of Transactions)"
-- Type: Pie Chart / Donut Chart
-- Dimension: risk_level   Metric: COUNT(*)
SELECT
    risk_level,
    COUNT(*)                                          AS transaction_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM `fraud-detection-500305-500517.reports.fraud_predictions`
GROUP BY risk_level
ORDER BY transaction_count DESC;

-- CHART 7: Fraud by Transaction Type
-- Chart name: "Fraud Transactions by Type"
-- Type: Bar Chart
-- Dimension: type   Metric: fraud_count
SELECT
    type,
    COUNT(*)                  AS total_transactions,
    COUNTIF(isFraud = 1)      AS actual_fraud_count,
    COUNTIF(predicted_isFraud = 1) AS predicted_fraud_count,
    ROUND(COUNTIF(isFraud = 1) * 100.0 / COUNT(*), 2) AS fraud_rate_pct
FROM `fraud-detection-500305-500517.reports.fraud_predictions`
GROUP BY type
ORDER BY actual_fraud_count DESC;
