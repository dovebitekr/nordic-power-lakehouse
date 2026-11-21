#!/usr/bin/env bash
# Exit immediately if any pipeline step fails
set -euo pipefail

# Move to the project root directory
cd "$(dirname "$0")"

echo "=== Starting Bronze Layer Ingestion and GCS Upload ==="
python ingestion/entsoe_generation.py --dataset all "$@"
python ingestion/entsoe.py "$@"
python ingestion/weather.py "$@"

echo "=== Starting Silver Layer PySpark Transformations ==="
python ingestion/silver_entsoe_generation.py
python ingestion/silver_entsoe.py
python ingestion/silver_weather.py

echo "=== Running Automated Data Quality Gate ==="
pytest -v tests/test_pipeline_integrity.py

echo "=== Loading Star Schema and 360 Views into BigQuery ==="
python ingestion/load_bigquery.py
