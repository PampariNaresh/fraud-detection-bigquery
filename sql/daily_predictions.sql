-- ============================================================
-- Daily Predictions SQL — Run standalone in BigQuery Console
-- Project: fraud-detection-500305
-- Uses fraud_boosted_tree on evaluation set (step > 600)
-- ============================================================

-- Run predictions and insert into reports table
INSERT INTO `fraud-detection-500305.reports.fraud_predictions`
SELECT
    CURRENT_TIMESTAMP()                                               AS prediction_date,
    f.step, f.type, f.amount, f.nameOrig, f.nameDest,
    f.oldbalanceOrg, f.newbalanceOrig,
    f.oldbalanceDest, f.newbalanceDest,
    CAST(p.predicted_isFraud AS INT64)                               AS predicted_isFraud,
    ROUND(p.predicted_isFraud_probs[OFFSET(1)].prob, 4)             AS fraud_probability,
    CASE
        WHEN p.predicted_isFraud_probs[OFFSET(1)].prob >= 0.7 THEN 'HIGH'
        WHEN p.predicted_isFraud_probs[OFFSET(1)].prob >= 0.4 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                                              AS risk_level,
    f.isFraud
FROM ML.PREDICT(
    MODEL `fraud-detection-500305.ml_models.fraud_boosted_tree`,
    (SELECT * FROM `fraud-detection-500305.features.transaction_features` WHERE step > 600)
) AS p
JOIN `fraud-detection-500305.features.transaction_features` AS f
    ON p.nameOrig = f.nameOrig
   AND p.step     = f.step
   AND p.amount   = f.amount
WHERE f.step > 600;

-- Write high-risk alerts
INSERT INTO `fraud-detection-500305.reports.high_risk_alerts`
(alert_date, step, type, amount, nameOrig, nameDest,
 fraud_probability, risk_level, alert_status, top_reason)
SELECT
    CURRENT_TIMESTAMP(),
    p.step, p.type, p.amount, p.nameOrig, p.nameDest,
    p.fraud_probability, p.risk_level,
    'PENDING_REVIEW',
    CASE
        WHEN f.orig_balance_zero_after = 1 THEN 'Account drained to zero'
        WHEN f.dest_balance_unchanged  = 1 THEN 'Destination balance unchanged'
        WHEN f.is_large_transaction    = 1 THEN 'Large transaction amount'
        WHEN f.is_transfer_or_cashout  = 1 THEN 'High-risk transaction type'
        ELSE 'Multiple fraud signals detected'
    END
FROM `fraud-detection-500305.reports.fraud_predictions` p
JOIN `fraud-detection-500305.features.transaction_features` f
    ON p.nameOrig = f.nameOrig
   AND p.step     = f.step
   AND p.amount   = f.amount
WHERE DATE(p.prediction_date) = CURRENT_DATE()
  AND p.risk_level = 'HIGH';

-- Write daily summary
INSERT INTO `fraud-detection-500305.reports.daily_summary`
(summary_date, total_transactions, flagged_count, high_risk_count,
 medium_risk_count, low_risk_count, total_amount_at_risk,
 avg_fraud_probability, model_used)
SELECT
    CURRENT_TIMESTAMP(),
    COUNT(*),
    COUNTIF(predicted_isFraud = 1),
    COUNTIF(risk_level = 'HIGH'),
    COUNTIF(risk_level = 'MEDIUM'),
    COUNTIF(risk_level = 'LOW'),
    ROUND(SUM(CASE WHEN risk_level IN ('HIGH','MEDIUM') THEN amount ELSE 0 END), 2),
    ROUND(AVG(fraud_probability), 4),
    'fraud_boosted_tree'
FROM `fraud-detection-500305.reports.fraud_predictions`
WHERE DATE(prediction_date) = CURRENT_DATE();

-- Verify predictions by risk level
SELECT
    risk_level,
    COUNT(*)                                               AS total,
    COUNTIF(isFraud = 1)                                  AS actual_fraud,
    ROUND(COUNTIF(isFraud = 1) / COUNT(*) * 100, 2)      AS hit_rate_pct,
    ROUND(AVG(fraud_probability), 4)                       AS avg_score
FROM `fraud-detection-500305.reports.fraud_predictions`
WHERE DATE(prediction_date) = CURRENT_DATE()
GROUP BY risk_level
ORDER BY hit_rate_pct DESC;

-- Top 20 highest risk transactions
SELECT
    type, amount, nameOrig, nameDest,
    fraud_probability, risk_level, isFraud
FROM `fraud-detection-500305.reports.fraud_predictions`
WHERE DATE(prediction_date) = CURRENT_DATE()
ORDER BY fraud_probability DESC
LIMIT 20;
