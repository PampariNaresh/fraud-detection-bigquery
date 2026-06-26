-- ============================================================
-- Transaction Fraud Detection System — Table Creation Script
-- Project: fraud-detection-500305-500517
-- Run once in BigQuery Console before triggering any DAGs
-- ============================================================

-- RAW DATASET
CREATE TABLE IF NOT EXISTS `fraud-detection-500305-500517.raw.transactions` (
    step            INTEGER,
    type            STRING,
    amount          FLOAT64,
    nameOrig        STRING,
    oldbalanceOrg   FLOAT64,
    newbalanceOrig  FLOAT64,
    nameDest        STRING,
    oldbalanceDest  FLOAT64,
    newbalanceDest  FLOAT64,
    isFraud         INTEGER,
    isFlaggedFraud  INTEGER
);

CREATE TABLE IF NOT EXISTS `fraud-detection-500305-500517.raw.ingestion_log` (
    run_date    TIMESTAMP,
    table_name  STRING,
    status      STRING,
    row_count   INTEGER
);

-- STAGING DATASET
CREATE TABLE IF NOT EXISTS `fraud-detection-500305-500517.staging.transactions_clean` (
    step            INT64,
    type            STRING,
    amount          FLOAT64,
    nameOrig        STRING,
    oldbalanceOrg   FLOAT64,
    newbalanceOrig  FLOAT64,
    nameDest        STRING,
    oldbalanceDest  FLOAT64,
    newbalanceDest  FLOAT64,
    isFraud         INT64,
    isFlaggedFraud  INT64
);

CREATE TABLE IF NOT EXISTS `fraud-detection-500305-500517.staging.validation_log` (
    run_date        TIMESTAMP,
    check_name      STRING,
    records_found   INT64,
    action_taken    STRING
);

-- FEATURES DATASET
CREATE TABLE IF NOT EXISTS `fraud-detection-500305-500517.features.transaction_features` (
    step                    INT64,
    type                    STRING,
    amount                  FLOAT64,
    nameOrig                STRING,
    oldbalanceOrg           FLOAT64,
    newbalanceOrig          FLOAT64,
    nameDest                STRING,
    oldbalanceDest          FLOAT64,
    newbalanceDest          FLOAT64,
    isFraud                 INT64,
    isFlaggedFraud          INT64,
    balance_diff_orig       FLOAT64,
    balance_diff_dest       FLOAT64,
    orig_balance_zero_after INT64,
    dest_balance_unchanged  INT64,
    amount_to_balance_ratio FLOAT64,
    is_large_transaction    INT64,
    hour_of_step            INT64,
    type_encoded            INT64,
    is_transfer_or_cashout  INT64,
    net_orig_change         FLOAT64,
    net_dest_change         FLOAT64
);

CREATE TABLE IF NOT EXISTS `fraud-detection-500305-500517.features.feature_log` (
    run_date            TIMESTAMP,
    total_records       INT64,
    fraud_records       INT64,
    avg_amount          FLOAT64,
    large_txn_count     INT64,
    zero_balance_count  INT64
);

-- ML MODELS DATASET
CREATE TABLE IF NOT EXISTS `fraud-detection-500305-500517.ml_models.model_evaluation_log` (
    run_date    TIMESTAMP,
    model_name  STRING,
    precision   FLOAT64,
    recall      FLOAT64,
    f1_score    FLOAT64,
    roc_auc     FLOAT64,
    log_loss    FLOAT64,
    status      STRING
);

CREATE TABLE IF NOT EXISTS `fraud-detection-500305-500517.ml_models.training_log` (
    run_date        TIMESTAMP,
    model_name      STRING,
    training_rows   INT64,
    eval_rows       INT64,
    threshold_used  FLOAT64,
    status          STRING
);

-- REPORTS DATASET
CREATE TABLE IF NOT EXISTS `fraud-detection-500305-500517.reports.fraud_predictions` (
    prediction_date     TIMESTAMP,
    step                INT64,
    type                STRING,
    amount              FLOAT64,
    nameOrig            STRING,
    nameDest            STRING,
    oldbalanceOrg       FLOAT64,
    newbalanceOrig      FLOAT64,
    oldbalanceDest      FLOAT64,
    newbalanceDest      FLOAT64,
    predicted_isFraud   INT64,
    fraud_probability   FLOAT64,
    risk_level          STRING,
    isFraud             INT64
);

CREATE TABLE IF NOT EXISTS `fraud-detection-500305-500517.reports.high_risk_alerts` (
    alert_date          TIMESTAMP,
    step                INT64,
    type                STRING,
    amount              FLOAT64,
    nameOrig            STRING,
    nameDest            STRING,
    fraud_probability   FLOAT64,
    risk_level          STRING,
    alert_status        STRING,
    top_reason          STRING
);

CREATE TABLE IF NOT EXISTS `fraud-detection-500305-500517.reports.daily_summary` (
    summary_date            TIMESTAMP,
    total_transactions      INT64,
    flagged_count           INT64,
    high_risk_count         INT64,
    medium_risk_count       INT64,
    low_risk_count          INT64,
    total_amount_at_risk    FLOAT64,
    avg_fraud_probability   FLOAT64,
    model_used              STRING
);
