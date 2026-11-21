"""
One-time initialisation Airflow DAG (@once):
Fetches initial historical Bronze data, runs Silver PySpark transformations,
seeds static Dimension tables via GCS, and builds BigQuery Star Schema & 360 views.
"""
from datetime import datetime, timedelta
from pathlib import Path
import sys
from airflow import DAG
from airflow.operators.bash import BashOperator

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from common.file_config import INITIALISE_END_DATE, INITIALISE_START_DATE

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
    dag_id="etl_initialise_data",
    default_args=default_args,
    description="One-time historical backfill and BigQuery Star Schema initialisation for Finland power analytics",
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    tags=["initialise", "gcp", "bigquery", "entsoe"],
) as dag:

    # Bronze extraction and GCS upload tasks
    bronze_entsoe_generation = BashOperator(
        task_id="bronze_entsoe_generation",
        bash_command=(
            f"cd {BASE_DIR} && python ingestion/entsoe_generation.py "
            f"--dataset all --start {INITIALISE_START_DATE} --end {INITIALISE_END_DATE}"
        ),
    )

    bronze_entsoe_price = BashOperator(
        task_id="bronze_entsoe_price",
        bash_command=(
            f"cd {BASE_DIR} && python ingestion/entsoe.py "
            f"--start {INITIALISE_START_DATE} --end {INITIALISE_END_DATE}"
        ),
    )

    bronze_weather = BashOperator(
        task_id="bronze_weather",
        bash_command=(
            f"cd {BASE_DIR} && python ingestion/weather.py "
            f"--start {INITIALISE_START_DATE} --end {INITIALISE_END_DATE}"
        ),
    )

    # Silver PySpark transformation tasks
    silver_entsoe_generation = BashOperator(
        task_id="silver_entsoe_generation",
        bash_command=f"cd {BASE_DIR} && python ingestion/silver_entsoe_generation.py",
    )

    silver_entsoe_price = BashOperator(
        task_id="silver_entsoe_price",
        bash_command=f"cd {BASE_DIR} && python ingestion/silver_entsoe.py",
    )

    silver_weather = BashOperator(
        task_id="silver_weather",
        bash_command=f"cd {BASE_DIR} && python ingestion/silver_weather.py",
    )

    # BigQuery warehouse initialisation task
    load_bigquery_initialise = BashOperator(
        task_id="load_bigquery_initialise",
        bash_command=f"cd {BASE_DIR} && python ingestion/load_bigquery.py",
    )

    # Task dependency graph
    bronze_entsoe_generation >> silver_entsoe_generation
    bronze_entsoe_price >> silver_entsoe_price
    bronze_weather >> silver_weather
    [silver_entsoe_generation, silver_entsoe_price, silver_weather] >> load_bigquery_initialise
