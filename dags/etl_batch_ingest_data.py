"""
Daily incremental batch Airflow DAG (06:00 UTC):
Ingests a rolling lookback window of Finland ENTSO-E A73/A75/A44 and weather data,
transforms to Silver Parquet via PySpark, and idempotently MERGEs into BigQuery.
"""
from datetime import datetime, timedelta
from pathlib import Path
import sys
from airflow import DAG
from airflow.operators.bash import BashOperator

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from common.file_config import BATCH_LOOKBACK_DAYS

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_batch_ingest_data",
    default_args=default_args,
    description="Daily incremental batch ingestion and BigQuery MERGE upsert for Finland power analytics",
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["daily_batch", "gcp", "bigquery", "entsoe"],
) as dag:

    lookback_start = f"{{{{ macros.ds_add(ds, -{BATCH_LOOKBACK_DAYS}) }}}}"

    # Rolling lookback Bronze extraction tasks
    batch_bronze_entsoe_generation = BashOperator(
        task_id="batch_bronze_entsoe_generation",
        bash_command=(
            f"cd {BASE_DIR} && python ingestion/entsoe_generation.py "
            f"--dataset all --start {lookback_start} --end {{{{ ds }}}} --overwrite"
        ),
    )

    batch_bronze_entsoe_price = BashOperator(
        task_id="batch_bronze_entsoe_price",
        bash_command=(
            f"cd {BASE_DIR} && python ingestion/entsoe.py "
            f"--start {lookback_start} --end {{{{ ds }}}} --overwrite"
        ),
    )

    batch_bronze_weather = BashOperator(
        task_id="batch_bronze_weather",
        bash_command=(
            f"cd {BASE_DIR} && python ingestion/weather.py "
            f"--start {lookback_start} --end {{{{ ds }}}} --overwrite"
        ),
    )

    # Silver PySpark transformation tasks
    batch_silver_entsoe_generation = BashOperator(
        task_id="batch_silver_entsoe_generation",
        bash_command=f"cd {BASE_DIR} && python ingestion/silver_entsoe_generation.py",
    )

    batch_silver_entsoe_price = BashOperator(
        task_id="batch_silver_entsoe_price",
        bash_command=f"cd {BASE_DIR} && python ingestion/silver_entsoe.py",
    )

    batch_silver_weather = BashOperator(
        task_id="batch_silver_weather",
        bash_command=f"cd {BASE_DIR} && python ingestion/silver_weather.py",
    )

    # BigQuery idempotent MERGE task
    batch_merge_bigquery = BashOperator(
        task_id="batch_merge_bigquery",
        bash_command=f"cd {BASE_DIR} && python ingestion/load_bigquery.py",
    )

    # Task dependency graph
    batch_bronze_entsoe_generation >> batch_silver_entsoe_generation
    batch_bronze_entsoe_price >> batch_silver_entsoe_price
    batch_bronze_weather >> batch_silver_weather
    [
        batch_silver_entsoe_generation,
        batch_silver_entsoe_price,
        batch_silver_weather,
    ] >> batch_merge_bigquery
