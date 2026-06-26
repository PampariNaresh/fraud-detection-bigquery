-- ============================================================
-- Feature Engineering SQL — Run standalone in BigQuery Console
-- Project: fraud-detection-500305
-- ============================================================

-- Step 1: Compute P95 threshold for large transactions
WITH p95 AS (
    SELECT APPROX_QUANTILES(amount, 100)[OFFSET(95)] AS threshold
    FROM `fraud-detection-500305-500517.staging.transactions_clean`
),

-- Step 2: Build feature table
features AS (
    SELECT
        t.*,
        (t.oldbalanceOrg - t.newbalanceOrig - t.amount)            AS balance_diff_orig,
        (t.newbalanceDest - t.oldbalanceDest - t.amount)           AS balance_diff_dest,
        IF(t.newbalanceOrig = 0 AND t.oldbalanceOrg > 0, 1, 0)    AS orig_balance_zero_after,
        IF(t.newbalanceDest = t.oldbalanceDest AND t.amount > 0, 1, 0) AS dest_balance_unchanged,
        SAFE_DIVIDE(t.amount, t.oldbalanceOrg + 1)                 AS amount_to_balance_ratio,
        IF(t.amount > p.threshold, 1, 0)                           AS is_large_transaction,
        MOD(t.step, 24)                                            AS hour_of_step,
        CASE t.type
            WHEN 'PAYMENT'  THEN 1
            WHEN 'TRANSFER' THEN 2
            WHEN 'CASH_OUT' THEN 3
            WHEN 'DEBIT'    THEN 4
            WHEN 'CASH_IN'  THEN 5
            ELSE 0
        END                                                        AS type_encoded,
        IF(t.type IN ('TRANSFER', 'CASH_OUT'), 1, 0)              AS is_transfer_or_cashout,
        (t.oldbalanceOrg - t.newbalanceOrig)                       AS net_orig_change,
        (t.newbalanceDest - t.oldbalanceDest)                      AS net_dest_change
    FROM `fraud-detection-500305-500517.staging.transactions_clean` t
    CROSS JOIN p95 p
)

-- Write to feature table
CREATE OR REPLACE TABLE `fraud-detection-500305-500517.features.transaction_features` AS
SELECT * FROM features;

-- Verify features
SELECT
    COUNT(*)                                 AS total_records,
    COUNTIF(isFraud = 1)                     AS fraud_count,
    COUNTIF(orig_balance_zero_after = 1)     AS zero_balance_orig_count,
    COUNTIF(dest_balance_unchanged = 1)      AS unchanged_dest_count,
    COUNTIF(is_large_transaction = 1)        AS large_txn_count,
    ROUND(AVG(amount_to_balance_ratio), 4)   AS avg_amount_ratio
FROM `fraud-detection-500305-500517.features.transaction_features`;
