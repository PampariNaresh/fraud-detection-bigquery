# Transaction Fraud Detection System

End-to-end fraud detection pipeline on GCP: 6.3M transactions → BigQuery ML → daily risk scores → Looker Studio dashboard.

## Stack

- **Orchestration:** Apache Airflow 2.8.0 (Docker + PostgreSQL)
- **Data warehouse:** Google BigQuery (project: `fraud-detection-500305`)
- **ML:** BigQuery ML — Logistic Regression, Boosted Tree, KMeans
- **Visualization:** Looker Studio (4-page dashboard)
- **Dataset:** PaySim — ~6.3M synthetic mobile money transactions

## Quick Start

### Prerequisites
- Docker + Docker Compose installed
- GCP service account key at `keys/sa-key.json`
- PaySim CSV uploaded to `gs://fraud-detection-raw-project/transactions/`

### 1. Start Airflow

```bash
# Initialize database (first time only)
docker compose run airflow-init

# Create admin user (first time only)
docker compose run airflow-webserver airflow users create \
  --username admin --password admin123 \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com

# Start all services
docker compose up -d

# Verify
docker compose ps
```

### 2. Configure GCP Connection

Open Airflow UI at `http://localhost:8080` (admin / admin123).

Go to **Admin → Connections → + Add**:

| Field | Value |
|---|---|
| Connection Id | `google_cloud_default` |
| Connection Type | `Google Cloud` |
| Project Id | `fraud-detection-500305` |
| Keyfile Path | `/opt/airflow/keys/sa-key.json` |

### 3. Create BigQuery Tables

Run `sql/create_tables.sql` in the BigQuery console once before triggering any DAGs.

### 4. Run the Pipeline

Trigger DAGs in order from the Airflow UI:

```
DAG 1 → DAG 2 → DAG 3 → DAG 4 → DAG 5 (auto-runs daily at 6AM)
```

## Pipeline

| DAG | Purpose | Schedule |
|---|---|---|
| `dag_01_ingest_transactions` | Load PaySim CSV from GCS → `raw.transactions` | Manual (once) |
| `dag_02_preprocess_transactions` | Clean & validate → `staging.transactions_clean` | Manual |
| `dag_03_feature_engineering` | Engineer 11 fraud features → `features.transaction_features` | Manual |
| `dag_04_train_models` | Train 3 BigQuery ML models | Manual |
| `dag_05_daily_predictions` | Score transactions, write alerts | Daily 6AM |

## BigQuery Datasets

| Dataset | Tables |
|---|---|
| `raw` | `transactions`, `ingestion_log` |
| `staging` | `transactions_clean`, `validation_log` |
| `features` | `transaction_features`, `feature_log` |
| `ml_models` | `model_evaluation_log`, `training_log` |
| `reports` | `fraud_predictions`, `high_risk_alerts`, `daily_summary` |

## Model Performance (expected)

| Model | F1 | AUC |
|---|---|---|
| `fraud_boosted_tree` | 0.89 | 0.97 |
| `fraud_logistic_regression` | 0.74 | 0.91 |

## Security

`keys/` and `data/` are in `.gitignore` — never commit the service account key.
