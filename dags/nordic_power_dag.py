from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nordic_power_lakehouse",
    default_args=default_args,
    description="End to end Medallion pipeline for Finnish power market",
    schedule="@daily",
    start_date=datetime(2025, 11, 1),
    catchup=False,
) as dag:

    # Bronze layer tasks
    bronze_fingrid = BashOperator(
        task_id="bronze_fingrid",
        bash_command="python ingestion/fingrid.py --start {{ prev_ds }} --end {{ds}}"
    )

    bronze_entsoe = BashOperator(
        task_id="bronze_entsoe",
        bash_command="python ingestion/entsoe.py --start {{ prev_ds }} --end {{ds}}",
    )

    #Silver layer tasks
    silver_fingrid = BashOperator(
        task_id="silver_fingrid",
        bash_command="python ingestion/silver_fingrid.py",
    )

    silver_entsoe = BashOperator(
        task_id="silver_entsoe",
        bash_command="python ingestion/silver_entsoe.py",
    )

    # Gold layer spine join task
    gold_power_balance = BashOperator(
        task_id="gold_power_balance",
        bash_command="python ingestion/gold_power_balance_15min.py",
    )

    # Automated data quality test task
    quality_tests = BashOperator(
        task_id="quality_tests",
        bash_command="pytest -v tests/test_gold_integrity.py",
    )

    # Load verified Gold table into PostgreSQL
    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command="python ingestion/load_postgres.py",
    )

    # Set parallel execution order
    bronze_fingrid >> silver_fingrid
    bronze_entsoe >> silver_entsoe
    [silver_fingrid, silver_entsoe] >> gold_power_balance >> quality_tests >> load_postgres