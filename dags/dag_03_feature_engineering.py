from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging

PROJECT_ID    = "fraud-detection-500305-500517"
GCP_CONN_ID   = "google_cloud_default"
STAGING_TABLE = f"{PROJECT_ID}.staging.transactions_clean"
FEATURE_TABLE = f"{PROJECT_ID}.features.transaction_features"
FEATURE_LOG   = f"{PROJECT_ID}.features.feature_log"

default_args = {
    "owner": "fraud-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def check_staging_exists(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    result = list(client.query(
        f"SELECT COUNT(*) as cnt FROM `{STAGING_TABLE}`"
    ).result())
    if result[0].cnt == 0:
        return "pipeline_stopped"
    return "compute_large_txn_threshold"

def compute_large_txn_threshold(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    result = list(client.query(f"""
        SELECT APPROX_QUANTILES(amount, 100)[OFFSET(95)] as p95
        FROM `{STAGING_TABLE}`
    """).result())
    p95 = result[0].p95
    context["ti"].xcom_push(key="p95_threshold", value=p95)
    logging.info(f"P95 threshold: {p95:,.2f}")

def build_feature_table(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    p95 = context["ti"].xcom_pull(key="p95_threshold")
    client.query(f"""
        CREATE OR REPLACE TABLE `{FEATURE_TABLE}` AS
        SELECT *,
            (oldbalanceOrg - newbalanceOrig - amount)            AS balance_diff_orig,
            (newbalanceDest - oldbalanceDest - amount)           AS balance_diff_dest,
            IF(newbalanceOrig=0 AND oldbalanceOrg>0, 1, 0)      AS orig_balance_zero_after,
            IF(newbalanceDest=oldbalanceDest AND amount>0, 1, 0) AS dest_balance_unchanged,
            SAFE_DIVIDE(amount, oldbalanceOrg + 1)              AS amount_to_balance_ratio,
            IF(amount > {p95}, 1, 0)                            AS is_large_transaction,
            MOD(step, 24)                                        AS hour_of_step,
            CASE type
                WHEN 'PAYMENT'  THEN 1
                WHEN 'TRANSFER' THEN 2
                WHEN 'CASH_OUT' THEN 3
                WHEN 'DEBIT'    THEN 4
                WHEN 'CASH_IN'  THEN 5
                ELSE 0
            END                                                  AS type_encoded,
            IF(type IN ('TRANSFER','CASH_OUT'), 1, 0)           AS is_transfer_or_cashout,
            (oldbalanceOrg - newbalanceOrig)                     AS net_orig_change,
            (newbalanceDest - oldbalanceDest)                    AS net_dest_change
        FROM `{STAGING_TABLE}`
    """).result()
    logging.info("Feature table created successfully")

def validate_features(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    checks = {
        "negative_ratio": f"SELECT COUNT(*) as cnt FROM `{FEATURE_TABLE}` WHERE amount_to_balance_ratio < 0",
        "invalid_type":   f"SELECT COUNT(*) as cnt FROM `{FEATURE_TABLE}` WHERE type_encoded = 0",
        "hour_range":     f"SELECT COUNT(*) as cnt FROM `{FEATURE_TABLE}` WHERE hour_of_step < 0 OR hour_of_step > 23",
    }
    for name, query in checks.items():
        count = list(client.query(query).result())[0].cnt
        if count > 0:
            raise ValueError(f"Feature validation failed: {name} has {count} bad records")
        logging.info(f"{name}: passed")

def log_feature_stats(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    client.query(f"""
        INSERT INTO `{FEATURE_LOG}`
        (run_date, total_records, fraud_records, avg_amount, large_txn_count, zero_balance_count)
        SELECT
            CURRENT_TIMESTAMP(),
            COUNT(*), COUNTIF(isFraud=1), ROUND(AVG(amount),2),
            COUNTIF(is_large_transaction=1), COUNTIF(orig_balance_zero_after=1)
        FROM `{FEATURE_TABLE}`
    """).result()

def print_feature_summary(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    result = list(client.query(f"""
        SELECT total_records, fraud_records, avg_amount, large_txn_count, zero_balance_count
        FROM `{FEATURE_LOG}`
        ORDER BY run_date DESC LIMIT 1
    """).result())
    if result:
        row = result[0]
        logging.info(f"Total records    : {row.total_records:,}")
        logging.info(f"Fraud records    : {row.fraud_records:,}")
        logging.info(f"Avg amount       : {row.avg_amount:,.2f}")
        logging.info(f"Large txns       : {row.large_txn_count:,}")
        logging.info(f"Zero balance orig: {row.zero_balance_count:,}")

with DAG(
    dag_id="dag_03_feature_engineering",
    description="Engineer fraud detection features from staging data",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["fraud-detection", "features"],
) as dag:

    check_staging = BranchPythonOperator(
        task_id="check_staging_exists",
        python_callable=check_staging_exists,
    )
    pipeline_stopped = EmptyOperator(task_id="pipeline_stopped")
    compute_threshold = PythonOperator(
        task_id="compute_large_txn_threshold",
        python_callable=compute_large_txn_threshold,
    )
    build_features = PythonOperator(
        task_id="build_feature_table",
        python_callable=build_feature_table,
    )
    validate = PythonOperator(
        task_id="validate_features",
        python_callable=validate_features,
    )
    log_stats = PythonOperator(
        task_id="log_feature_stats",
        python_callable=log_feature_stats,
    )
    summary = PythonOperator(
        task_id="print_feature_summary",
        python_callable=print_feature_summary,
    )

    check_staging >> pipeline_stopped
    check_staging >> compute_threshold >> build_features >> validate >> log_stats >> summary
