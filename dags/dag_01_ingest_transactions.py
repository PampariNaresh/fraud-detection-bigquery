from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

PROJECT_ID    = "fraud-detection-500305-500517"
BUCKET_NAME   = "fraud-detection-raw-500305-500517"
GCS_FILE_PATH = "transactions/PS_20174392719_1491204439457_log.csv"
BQ_DATASET    = "raw"
BQ_TABLE      = "transactions"
GCP_CONN_ID   = "google_cloud_default"

default_args = {
    "owner": "fraud-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

SCHEMA_FIELDS = [
    {"name": "step",           "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "type",           "type": "STRING",  "mode": "NULLABLE"},
    {"name": "amount",         "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "nameOrig",       "type": "STRING",  "mode": "NULLABLE"},
    {"name": "oldbalanceOrg",  "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "newbalanceOrig", "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "nameDest",       "type": "STRING",  "mode": "NULLABLE"},
    {"name": "oldbalanceDest", "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "newbalanceDest", "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "isFraud",        "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "isFlaggedFraud", "type": "INTEGER", "mode": "NULLABLE"},
]

def validate_loaded_data(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT COUNT(*) as total_rows,
               SUM(isFraud) as fraud_rows,
               ROUND(SUM(isFraud) / COUNT(*) * 100, 4) as fraud_pct
        FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
    """
    result = client.query(query).result()
    for row in result:
        logging.info(f"Total rows: {row.total_rows:,}")
        logging.info(f"Fraud rows: {row.fraud_rows:,}")
        logging.info(f"Fraud pct : {row.fraud_pct}%")
        if row.total_rows == 0:
            raise ValueError("No rows loaded into BigQuery!")
        if row.fraud_pct > 10:
            raise ValueError(f"Fraud % too high ({row.fraud_pct}%)")
    logging.info("Validation passed!")

with DAG(
    dag_id="dag_01_ingest_transactions",
    description="Load PaySim transactions from GCS into BigQuery",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["fraud-detection", "ingestion"],
) as dag:

    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=[GCS_FILE_PATH],
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}",
        schema_fields=SCHEMA_FIELDS,
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=GCP_CONN_ID,
    )

    validate_data = PythonOperator(
        task_id="validate_loaded_data",
        python_callable=validate_loaded_data,
    )

    log_ingestion = BigQueryInsertJobOperator(
        task_id="log_ingestion_run",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{PROJECT_ID}.raw.ingestion_log`
                    (run_date, table_name, status, row_count)
                    SELECT
                        CURRENT_TIMESTAMP(),
                        '{BQ_DATASET}.{BQ_TABLE}',
                        'SUCCESS',
                        COUNT(*)
                    FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
                """,
                "useLegacySql": False,
            }
        },
    )

    load_to_bigquery >> validate_data >> log_ingestion
