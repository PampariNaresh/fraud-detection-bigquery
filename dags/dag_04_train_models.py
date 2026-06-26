from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging

PROJECT_ID    = "fraud-detection-500305-500517"
GCP_CONN_ID   = "google_cloud_default"
FEATURE_TABLE = f"{PROJECT_ID}.features.transaction_features"
EVAL_LOG      = f"{PROJECT_ID}.ml_models.model_evaluation_log"
TRAIN_LOG     = f"{PROJECT_ID}.ml_models.training_log"

default_args = {
    "owner": "fraud-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

def check_feature_table(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    result = list(client.query(
        f"SELECT COUNT(*) as cnt FROM `{FEATURE_TABLE}`"
    ).result())
    if result[0].cnt == 0:
        return "pipeline_stopped"
    return "train_logistic_regression"

def evaluate_model(model_name, **context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    result = list(client.query(f"""
        SELECT precision, recall, f1_score, roc_auc, log_loss
        FROM ML.EVALUATE(
            MODEL `{PROJECT_ID}.ml_models.{model_name}`,
            (SELECT * FROM `{FEATURE_TABLE}` WHERE step > 100)
        )
    """).result())
    if not result:
        logging.warning(f"No evaluation results for {model_name}")
        return
    row = result[0]
    precision = round(row.precision, 4)
    recall    = round(row.recall, 4)
    f1        = round(row.f1_score, 4)
    auc       = round(row.roc_auc, 4)
    ll        = round(row.log_loss, 4)
    logging.info(f"{model_name} — P={precision}, R={recall}, F1={f1}, AUC={auc}")
    client.query(f"""
        INSERT INTO `{EVAL_LOG}`
        (run_date, model_name, precision, recall, f1_score, roc_auc, log_loss, status)
        VALUES (CURRENT_TIMESTAMP(), '{model_name}', {precision}, {recall}, {f1}, {auc}, {ll}, 'SUCCESS')
    """).result()
    context["ti"].xcom_push(key=f"{model_name}_f1",  value=f1)
    context["ti"].xcom_push(key=f"{model_name}_auc", value=auc)

def evaluate_logistic(**context):
    evaluate_model("fraud_logistic_regression", **context)

def evaluate_boosted(**context):
    evaluate_model("fraud_boosted_tree", **context)

def log_training_run(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    train_count = list(client.query(
        f"SELECT COUNT(*) as cnt FROM `{FEATURE_TABLE}` WHERE step <= 100"
    ).result())[0].cnt
    eval_count = list(client.query(
        f"SELECT COUNT(*) as cnt FROM `{FEATURE_TABLE}` WHERE step > 100"
    ).result())[0].cnt
    for model in ["fraud_logistic_regression", "fraud_boosted_tree", "fraud_kmeans_anomaly"]:
        client.query(f"""
            INSERT INTO `{TRAIN_LOG}`
            (run_date, model_name, training_rows, eval_rows, threshold_used, status)
            VALUES (CURRENT_TIMESTAMP(), '{model}', {train_count}, {eval_count}, 0.5, 'SUCCESS')
        """).result()

def compare_models(**context):
    for model in ["fraud_logistic_regression", "fraud_boosted_tree"]:
        f1  = context["ti"].xcom_pull(key=f"{model}_f1")
        auc = context["ti"].xcom_pull(key=f"{model}_auc")
        logging.info(f"{model}: F1={f1}, AUC={auc}")
    logging.info("Primary model for predictions: fraud_boosted_tree")

TRAIN_FEATURES = """
    balance_diff_orig, balance_diff_dest, orig_balance_zero_after,
    dest_balance_unchanged, amount_to_balance_ratio, is_large_transaction,
    hour_of_step, type_encoded, is_transfer_or_cashout,
    net_orig_change, net_dest_change, isFraud
"""

with DAG(
    dag_id="dag_04_train_models",
    description="Train 3 BigQuery ML fraud detection models",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["fraud-detection", "ml-training"],
) as dag:

    check_features = BranchPythonOperator(
        task_id="check_feature_table",
        python_callable=check_feature_table,
    )
    pipeline_stopped = EmptyOperator(task_id="pipeline_stopped")

    train_logistic = BigQueryInsertJobOperator(
        task_id="train_logistic_regression",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE MODEL `{PROJECT_ID}.ml_models.fraud_logistic_regression`
                    OPTIONS(
                        model_type       = 'LOGISTIC_REG',
                        input_label_cols = ['isFraud'],
                        max_iterations   = 20,
                        class_weights    = [STRUCT('0', 1.0), STRUCT('1', 10.0)]
                    ) AS
                    SELECT {TRAIN_FEATURES}
                    FROM `{FEATURE_TABLE}`
                    WHERE step <= 100
                """,
                "useLegacySql": False,
            }
        },
    )

    eval_logistic = PythonOperator(
        task_id="evaluate_logistic_regression",
        python_callable=evaluate_logistic,
    )

    train_boosted = BigQueryInsertJobOperator(
        task_id="train_boosted_tree",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE MODEL `{PROJECT_ID}.ml_models.fraud_boosted_tree`
                    OPTIONS(
                        model_type            = 'RANDOM_FOREST_CLASSIFIER',
                        input_label_cols      = ['isFraud'],
                        num_parallel_tree     = 10,
                        max_tree_depth        = 4,
                        subsample             = 0.8,
                        auto_class_weights    = TRUE,
                        data_split_method     = 'AUTO_SPLIT',
                        enable_global_explain = TRUE
                    ) AS
                    SELECT {TRAIN_FEATURES}
                    FROM `{FEATURE_TABLE}`
                    WHERE step <= 100
                """,
                "useLegacySql": False,
            }
        },
    )

    eval_boosted = PythonOperator(
        task_id="evaluate_boosted_tree",
        python_callable=evaluate_boosted,
    )

    train_kmeans = BigQueryInsertJobOperator(
        task_id="train_kmeans_anomaly",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE MODEL `{PROJECT_ID}.ml_models.fraud_kmeans_anomaly`
                    OPTIONS(
                        model_type           = 'KMEANS',
                        num_clusters         = 8,
                        kmeans_init_method   = 'KMEANS++',
                        standardize_features = TRUE
                    ) AS
                    SELECT balance_diff_orig, balance_diff_dest, amount_to_balance_ratio,
                           orig_balance_zero_after, dest_balance_unchanged,
                           is_large_transaction, is_transfer_or_cashout
                    FROM `{FEATURE_TABLE}`
                    WHERE step <= 100
                """,
                "useLegacySql": False,
            }
        },
    )

    log_run = PythonOperator(
        task_id="log_training_run",
        python_callable=log_training_run,
    )

    compare = PythonOperator(
        task_id="compare_models",
        python_callable=compare_models,
    )

    check_features >> pipeline_stopped
    check_features >> train_logistic >> eval_logistic
    eval_logistic  >> train_boosted
    eval_logistic  >> train_kmeans
    train_boosted  >> eval_boosted
    eval_boosted   >> log_run
    train_kmeans   >> log_run
    log_run        >> compare
