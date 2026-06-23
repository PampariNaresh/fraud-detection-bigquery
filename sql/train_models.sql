-- ============================================================
-- Model Training SQL — Run standalone in BigQuery Console
-- Project: fraud-detection-500305
-- Training split: step <= 600 | Eval split: step > 600
-- ============================================================

-- MODEL 1: Logistic Regression (Baseline)
CREATE OR REPLACE MODEL `fraud-detection-500305.ml_models.fraud_logistic_regression`
OPTIONS(
    model_type       = 'LOGISTIC_REG',
    input_label_cols = ['isFraud'],
    max_iterations   = 20,
    class_weights    = [STRUCT('0', 1.0), STRUCT('1', 10.0)]
) AS
SELECT
    balance_diff_orig, balance_diff_dest, orig_balance_zero_after,
    dest_balance_unchanged, amount_to_balance_ratio, is_large_transaction,
    hour_of_step, type_encoded, is_transfer_or_cashout,
    net_orig_change, net_dest_change, isFraud
FROM `fraud-detection-500305.features.transaction_features`
WHERE step <= 600;

-- Evaluate logistic regression
SELECT precision, recall, f1_score, roc_auc, log_loss
FROM ML.EVALUATE(
    MODEL `fraud-detection-500305.ml_models.fraud_logistic_regression`,
    (SELECT * FROM `fraud-detection-500305.features.transaction_features` WHERE step > 600)
);

-- MODEL 2: Boosted Tree Classifier (Primary Model)
CREATE OR REPLACE MODEL `fraud-detection-500305.ml_models.fraud_boosted_tree`
OPTIONS(
    model_type            = 'BOOSTED_TREE_CLASSIFIER',
    input_label_cols      = ['isFraud'],
    num_parallel_tree     = 50,
    max_tree_depth        = 6,
    min_split_loss        = 0.01,
    learn_rate            = 0.1,
    auto_class_weights    = TRUE,
    data_split_method     = 'AUTO_SPLIT',
    enable_global_explain = TRUE
) AS
SELECT
    balance_diff_orig, balance_diff_dest, orig_balance_zero_after,
    dest_balance_unchanged, amount_to_balance_ratio, is_large_transaction,
    hour_of_step, type_encoded, is_transfer_or_cashout,
    net_orig_change, net_dest_change, isFraud
FROM `fraud-detection-500305.features.transaction_features`
WHERE step <= 600;

-- Evaluate boosted tree
SELECT precision, recall, f1_score, roc_auc, log_loss
FROM ML.EVALUATE(
    MODEL `fraud-detection-500305.ml_models.fraud_boosted_tree`,
    (SELECT * FROM `fraud-detection-500305.features.transaction_features` WHERE step > 600)
);

-- Feature importance for boosted tree
SELECT feature, attribution
FROM ML.GLOBAL_EXPLAIN(MODEL `fraud-detection-500305.ml_models.fraud_boosted_tree`)
ORDER BY attribution DESC;

-- MODEL 3: KMeans Anomaly Detection (Unsupervised)
CREATE OR REPLACE MODEL `fraud-detection-500305.ml_models.fraud_kmeans_anomaly`
OPTIONS(
    model_type           = 'KMEANS',
    num_clusters         = 8,
    kmeans_init_method   = 'KMEANS++',
    standardize_features = TRUE
) AS
SELECT
    balance_diff_orig, balance_diff_dest, amount_to_balance_ratio,
    orig_balance_zero_after, dest_balance_unchanged,
    is_large_transaction, is_transfer_or_cashout
FROM `fraud-detection-500305.features.transaction_features`
WHERE step <= 600;

-- List all trained models
SELECT * FROM `fraud-detection-500305.ml_models.INFORMATION_SCHEMA.MODELS`;

-- Compare model evaluation results
SELECT model_name, precision, recall, f1_score, roc_auc
FROM `fraud-detection-500305.ml_models.model_evaluation_log`
ORDER BY f1_score DESC;
