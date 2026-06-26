from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging

PROJECT_ID    = "fraud-detection-500305-500517"
GCP_CONN_ID   = "google_cloud_default"
FEATURE_TABLE = f"{PROJECT_ID}.features.transaction_features"
MODEL         = f"{PROJECT_ID}.ml_models.fraud_logistic_regression"
PREDICTIONS   = f"{PROJECT_ID}.reports.fraud_predictions"
ALERTS        = f"{PROJECT_ID}.reports.high_risk_alerts"
SUMMARY       = f"{PROJECT_ID}.reports.daily_summary"

default_args = {
    "owner": "fraud-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def check_model_exists(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    models = [m.model_id for m in client.list_models(f"{PROJECT_ID}.ml_models")]
    if "fraud_logistic_regression" not in models:
        return "pipeline_stopped"
    return "run_predictions"

def check_prediction_results(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    result = list(client.query(f"""
        SELECT COUNT(*) as cnt
        FROM `{PREDICTIONS}`
        WHERE DATE(prediction_date) = CURRENT_DATE()
    """).result())
    count = result[0].cnt
    logging.info(f"Today's predictions: {count:,}")
    context["ti"].xcom_push(key="prediction_count", value=count)

def get_explainability(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    result = list(client.query(f"""
        SELECT processed_input AS feature, weight
        FROM ML.WEIGHTS(MODEL `{MODEL}`)
        ORDER BY ABS(weight) DESC
        LIMIT 5
    """).result())
    logging.info("Top 5 fraud features (logistic regression weights):")
    for row in result:
        logging.info(f"  {row.feature}: {row.weight:.4f}")

def send_alert_notification(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    result = list(client.query(f"""
        SELECT
            COUNT(*) as high_risk,
            ROUND(SUM(amount), 2) as total_amount
        FROM `{ALERTS}`
        WHERE DATE(alert_date) = CURRENT_DATE()
          AND risk_level = 'HIGH'
    """).result())
    if result:
        row = result[0]
        logging.info(f"HIGH RISK: {row.high_risk} transactions, ${row.total_amount:,.2f} at risk")
    logging.info("Alerts written to high_risk_alerts table — ready for investigator review")

with DAG(
    dag_id="dag_05_daily_predictions",
    description="Score transactions daily and route high-risk alerts to investigators",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["fraud-detection", "predictions"],
) as dag:

    check_model = BranchPythonOperator(
        task_id="check_model_exists",
        python_callable=check_model_exists,
    )
    pipeline_stopped = EmptyOperator(task_id="pipeline_stopped")

    run_predictions = BigQueryInsertJobOperator(
        task_id="run_predictions",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{PREDICTIONS}`
                    SELECT
                        CURRENT_TIMESTAMP()                                               AS prediction_date,
                        f.step, f.type, f.amount, f.nameOrig, f.nameDest,
                        f.oldbalanceOrg, f.newbalanceOrig,
                        f.oldbalanceDest, f.newbalanceDest,
                        CAST(p.predicted_isFraud AS INT64)                               AS predicted_isFraud,
                        ROUND((SELECT prob FROM UNNEST(p.predicted_isFraud_probs) WHERE label = 1), 4) AS fraud_probability,
                        CASE
                            WHEN (SELECT prob FROM UNNEST(p.predicted_isFraud_probs) WHERE label = 1) >= 0.7 THEN 'HIGH'
                            WHEN (SELECT prob FROM UNNEST(p.predicted_isFraud_probs) WHERE label = 1) >= 0.4 THEN 'MEDIUM'
                            ELSE 'LOW'
                        END                                                              AS risk_level,
                        f.isFraud
                    FROM ML.PREDICT(
                        MODEL `{MODEL}`,
                        (SELECT * FROM `{FEATURE_TABLE}` WHERE step > 600)
                    ) AS p
                    JOIN `{FEATURE_TABLE}` AS f
                        ON p.nameOrig = f.nameOrig
                       AND p.step     = f.step
                       AND p.amount   = f.amount
                    WHERE f.step > 600
                """,
                "useLegacySql": False,
            }
        },
    )

    check_results = PythonOperator(
        task_id="check_prediction_results",
        python_callable=check_prediction_results,
    )

    get_explain = PythonOperator(
        task_id="get_explainability",
        python_callable=get_explainability,
    )

    write_summary = BigQueryInsertJobOperator(
        task_id="write_daily_summary",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{SUMMARY}`
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
                        'fraud_logistic_regression'
                    FROM `{PREDICTIONS}`
                    WHERE DATE(prediction_date) = CURRENT_DATE()
                """,
                "useLegacySql": False,
            }
        },
    )

    write_alerts = BigQueryInsertJobOperator(
        task_id="write_high_risk_alerts",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{ALERTS}`
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
                    FROM `{PREDICTIONS}` p
                    JOIN `{FEATURE_TABLE}` f
                        ON p.nameOrig = f.nameOrig
                       AND p.step     = f.step
                       AND p.amount   = f.amount
                    WHERE DATE(p.prediction_date) = CURRENT_DATE()
                      AND p.risk_level = 'HIGH'
                """,
                "useLegacySql": False,
            }
        },
    )

    notify = PythonOperator(
        task_id="send_alert_notification",
        python_callable=send_alert_notification,
    )

    check_model      >> pipeline_stopped
    check_model      >> run_predictions >> check_results
    check_results    >> get_explain
    check_results    >> write_summary
    get_explain      >> write_alerts
    write_summary    >> notify
    write_alerts     >> notify
