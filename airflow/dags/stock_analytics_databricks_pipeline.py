from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os

# Pipeline defaults (retries & reliability)
default_args = {
    "owner": "airflow_user",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# Detect fresh stock data arrival
def detect_data_update(**context):
    source_dir = "/opt/airflow/data/raw/stocks"
    last_processed = Variable.get("stocks_last_processed_ts", default_var=None)

    if not os.path.exists(source_dir):
        context["ti"].xcom_push(key="proceed", value=False)
        return

    files = [os.path.join(source_dir, f) for f in os.listdir(source_dir)]
    latest_ts = max(os.path.getmtime(f) for f in files)

    if last_processed is None or latest_ts > float(last_processed):
        context["ti"].xcom_push(key="proceed", value=True)
    else:
        context["ti"].xcom_push(key="proceed", value=False)

# Persist pipeline run metadata
def mark_pipeline_success():
    Variable.set(
        "stocks_last_processed_ts",
        str(datetime.now().timestamp())
    )

# DAG Definition
with DAG(
    dag_id="stock_market_medallion_pipeline",
    description="Automated stock ETL using Bronze, Silver & Gold layers",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",  
    catchup=False,
    tags=["stocks", "etl", "databricks", "medallion"],
) as dag:

    # Step 1: Validate new data availability
    bronze_data_validation = PythonOperator(
        task_id="bronze_data_validation",
        python_callable=detect_data_update
    )

    # Step 2: Bronze Layer – Raw ingestion
    bronze_load = BashOperator(
        task_id="bronze_raw_ingestion",
        bash_command="python /opt/airflow/scripts/ingestion_cleaning.py"
    )

    # Step 3: Silver Layer – Cleaning & normalization
    silver_transform = BashOperator(
        task_id="silver_data_standardization",
        bash_command="python /opt/airflow/scripts/ingestion_cleaning.py"
    )

    # Step 4: Gold Layer – Databricks analytics
    gold_insights = DatabricksSubmitRunOperator(
        task_id="gold_analytics_execution",
        databricks_conn_id="databricks_default",
        existing_cluster_id="1226-111154-b05vqw7v",
        notebook_task={
            "notebook_path": "/Users/22071a12c2@vnrvjiet.in/gold_analytics"
        }
    )

    # Step 5: Update run marker
    finalize_run = PythonOperator(
        task_id="finalize_pipeline_run",
        python_callable=mark_pipeline_success
    )

    bronze_data_validation >> bronze_load >> silver_transform >> gold_insights >> finalize_run
