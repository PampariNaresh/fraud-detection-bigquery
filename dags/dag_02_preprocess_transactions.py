from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging

PROJECT_ID  = "fraud-detection-500305"
GCP_CONN_ID = "google_cloud_default"
RAW_TABLE   = f"{PROJECT_ID}.raw.transactions"
CLEAN_TABLE = f"{PROJECT_ID}.staging.transactions_clean"
VAL_LOG     = f"{PROJECT_ID}.staging.validation_log"

default_args = {
    "owner": "fraud-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def check_raw_data_exists(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    result = list(client.query(
        f"SELECT COUNT(*) as cnt FROM `{RAW_TABLE}`"
    ).result())
    if result[0].cnt == 0:
        return "pipeline_stopped"
    return ["run_null_check", "run_negative_check", "run_duplicate_check", "run_balance_check"]

def count_bad_records(check_name, query, **context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    result = list(client.query(query).result())
    count = result[0].cnt
    context["ti"].xcom_push(key=check_name, value=count)
    logging.info(f"{check_name}: {count:,} bad records")

def run_null_check(**context):
    count_bad_records("null_amount_records", f"""
        SELECT COUNT(*) as cnt FROM `{RAW_TABLE}`
        WHERE amount IS NULL OR nameOrig IS NULL
           OR nameDest IS NULL OR type IS NULL
    """, **context)

def run_negative_check(**context):
    count_bad_records("negative_amount_records",
        f"SELECT COUNT(*) as cnt FROM `{RAW_TABLE}` WHERE amount <= 0",
        **context)

def run_duplicate_check(**context):
    count_bad_records("duplicate_records", f"""
        SELECT COUNT(*) - COUNT(DISTINCT CONCAT(
            CAST(step AS STRING), nameOrig,
            CAST(amount AS STRING), nameDest
        )) as cnt FROM `{RAW_TABLE}`
    """, **context)

def run_balance_check(**context):
    count_bad_records("impossible_balance", f"""
        SELECT COUNT(*) as cnt FROM `{RAW_TABLE}`
        WHERE type IN ('TRANSFER','CASH_OUT')
          AND amount > oldbalanceOrg + 1
          AND oldbalanceOrg > 0
    """, **context)

def log_validation_results(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    checks = [
        ("null_amount_records",     "Removed nulls"),
        ("negative_amount_records", "Removed negatives"),
        ("duplicate_records",       "Removed duplicates"),
        ("impossible_balance",      "Removed impossible balances"),
    ]
    for check_name, action in checks:
        count = context["ti"].xcom_pull(key=check_name) or 0
        client.query(f"""
            INSERT INTO `{VAL_LOG}`
            (run_date, check_name, records_found, action_taken)
            VALUES (CURRENT_TIMESTAMP(), '{check_name}', {count}, '{action}')
        """).result()

with DAG(
    dag_id="dag_02_preprocess_transactions",
    description="Clean raw transactions and write to staging",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["fraud-detection", "preprocessing"],
) as dag:

    check_raw_exists = BranchPythonOperator(
        task_id="check_raw_data_exists",
        python_callable=check_raw_data_exists,
    )
    pipeline_stopped  = EmptyOperator(task_id="pipeline_stopped")
    null_check        = PythonOperator(task_id="run_null_check",      python_callable=run_null_check)
    negative_check    = PythonOperator(task_id="run_negative_check",  python_callable=run_negative_check)
    duplicate_check   = PythonOperator(task_id="run_duplicate_check", python_callable=run_duplicate_check)
    balance_check     = PythonOperator(task_id="run_balance_check",   python_callable=run_balance_check)

    clean_and_load = BigQueryInsertJobOperator(
        task_id="clean_and_load_to_staging",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{CLEAN_TABLE}` AS
                    WITH deduplicated AS (
                        SELECT *,
                            ROW_NUMBER() OVER (
                                PARTITION BY step, nameOrig, CAST(amount AS STRING), nameDest
                                ORDER BY step
                            ) AS row_num
                        FROM `{RAW_TABLE}`
                    )
                    SELECT step, type, amount, nameOrig, oldbalanceOrg,
                           newbalanceOrig, nameDest, oldbalanceDest,
                           newbalanceDest, isFraud, isFlaggedFraud
                    FROM deduplicated
                    WHERE row_num = 1
                      AND amount IS NOT NULL
                      AND nameOrig IS NOT NULL
                      AND nameDest IS NOT NULL
                      AND type IS NOT NULL
                      AND amount > 0
                      AND type IN ('PAYMENT','TRANSFER','CASH_OUT','DEBIT','CASH_IN')
                """,
                "useLegacySql": False,
            }
        },
    )

    log_results = PythonOperator(
        task_id="log_validation_results",
        python_callable=log_validation_results,
    )

    final_check = BigQueryInsertJobOperator(
        task_id="final_staging_check",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    SELECT COUNT(*) as clean_rows,
                           COUNTIF(isFraud=1) as fraud_rows,
                           COUNTIF(amount<=0) as bad_amount_rows
                    FROM `{CLEAN_TABLE}`
                """,
                "useLegacySql": False,
            }
        },
    )

    check_raw_exists >> pipeline_stopped
    check_raw_exists >> [null_check, negative_check, duplicate_check, balance_check]
    [null_check, negative_check, duplicate_check, balance_check] >> clean_and_load
    clean_and_load >> log_results >> final_check
