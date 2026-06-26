-- ============================================================
-- PAGE 3 — Model Health & Feature Insights
-- Looker Studio Data Source: ml_models.model_evaluation_log
--                            ml_models.training_log
--                            reports.fraud_predictions
--                            features.transaction_features
-- ============================================================

-- CHART 1: KPI Scorecard — Latest F1 Score
-- Chart name: "Model F1 Score (Latest)"
-- Type: Scorecard
SELECT
    model_name,
    ROUND(f1_score, 4)  AS f1_score,
    ROUND(precision, 4) AS precision,
    ROUND(recall, 4)    AS recall,
    ROUND(roc_auc, 4)   AS roc_auc
FROM `fraud-detection-500305-500517.ml_models.model_evaluation_log`
ORDER BY run_date DESC
LIMIT 1;

-- CHART 2: KPI Scorecard — Precision
-- Chart name: "Precision (Latest)"
-- Type: Scorecard

-- CHART 3: KPI Scorecard — Recall
-- Chart name: "Recall (Latest)"
-- Type: Scorecard

-- CHART 4: KPI Scorecard — AUC-ROC
-- Chart name: "AUC-ROC Score (Latest)"
-- Type: Scorecard

-- CHART 5: Model Evaluation History
-- Chart name: "Model Performance Over Time"
-- Type: Line Chart
-- Dimension: run_date   Metrics: f1_score, roc_auc, precision, recall
SELECT
    DATE(run_date) AS run_date,
    model_name,
    ROUND(f1_score, 4)  AS f1_score,
    ROUND(precision, 4) AS precision,
    ROUND(recall, 4)    AS recall,
    ROUND(roc_auc, 4)   AS roc_auc,
    ROUND(log_loss, 4)  AS log_loss
FROM `fraud-detection-500305-500517.ml_models.model_evaluation_log`
ORDER BY run_date DESC;

-- CHART 6: Model Comparison Table
-- Chart name: "Model Comparison (All Runs)"
-- Type: Table
SELECT
    model_name,
    DATE(run_date)       AS run_date,
    ROUND(f1_score, 4)   AS f1_score,
    ROUND(precision, 4)  AS precision,
    ROUND(recall, 4)     AS recall,
    ROUND(roc_auc, 4)    AS roc_auc,
    ROUND(log_loss, 4)   AS log_loss,
    status
FROM `fraud-detection-500305-500517.ml_models.model_evaluation_log`
ORDER BY run_date DESC, f1_score DESC;

-- CHART 7: Training Volume Over Time
-- Chart name: "Training vs Evaluation Row Counts"
-- Type: Grouped Bar Chart
SELECT
    model_name,
    DATE(run_date)  AS run_date,
    training_rows,
    eval_rows,
    threshold_used
FROM `fraud-detection-500305-500517.ml_models.training_log`
ORDER BY run_date DESC;

-- CHART 8: Fraud Probability Distribution
-- Chart name: "Prediction Score Distribution"
-- Type: Bar Chart (histogram buckets)
-- Calculated field in Looker: prob_bucket
SELECT
    CASE
        WHEN fraud_probability < 0.2 THEN '0.0 – 0.2'
        WHEN fraud_probability < 0.4 THEN '0.2 – 0.4'
        WHEN fraud_probability < 0.6 THEN '0.4 – 0.6'
        WHEN fraud_probability < 0.8 THEN '0.6 – 0.8'
        ELSE                              '0.8 – 1.0'
    END                          AS prob_bucket,
    COUNT(*)                     AS transaction_count,
    COUNTIF(isFraud = 1)         AS actual_fraud_count,
    ROUND(COUNTIF(isFraud = 1) * 100.0 / COUNT(*), 2) AS fraud_hit_rate_pct
FROM `fraud-detection-500305-500517.reports.fraud_predictions`
GROUP BY prob_bucket
ORDER BY prob_bucket;

-- CHART 9: Feature Importance Proxy (Fraud Rate per Feature Bucket)
-- Chart name: "Fraud Rate by Transaction Type & Hour"
-- Type: Heatmap / Table
SELECT
    type,
    CASE
        WHEN MOD(step, 24) BETWEEN 0  AND 5  THEN 'Night (0–5)'
        WHEN MOD(step, 24) BETWEEN 6  AND 11 THEN 'Morning (6–11)'
        WHEN MOD(step, 24) BETWEEN 12 AND 17 THEN 'Afternoon (12–17)'
        ELSE                                       'Evening (18–23)'
    END                                           AS time_of_day,
    COUNT(*)                                      AS transaction_count,
    COUNTIF(isFraud = 1)                          AS fraud_count,
    ROUND(COUNTIF(isFraud = 1) * 100.0 / COUNT(*), 4) AS fraud_rate_pct
FROM `fraud-detection-500305-500517.features.transaction_features`
GROUP BY type, time_of_day
ORDER BY fraud_rate_pct DESC;

-- CHART 10: Amount vs Fraud Probability Scatter
-- Chart name: "Transaction Amount vs Fraud Score"
-- Type: Scatter Chart
-- X-axis: amount   Y-axis: fraud_probability   Color: risk_level
SELECT
    type,
    ROUND(amount, 2)            AS amount,
    ROUND(fraud_probability, 4) AS fraud_probability,
    risk_level,
    isFraud                     AS actual_fraud_label
FROM `fraud-detection-500305-500517.reports.fraud_predictions`
WHERE fraud_probability > 0.1
ORDER BY fraud_probability DESC
LIMIT 5000;
