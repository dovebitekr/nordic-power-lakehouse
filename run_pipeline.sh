#!/usr/bin/env bash
# Exit immediately if any pipeline step fails
set -euo pipefail

# Move to the project root directory
cd "$(dirname "$0")"

echo "=== Starting Bronze Layer Ingestion ==="
python ingestion/fingrid.py "$@"
python ingestion/entsoe.py "$@"

echo "=== Starting Silver Layer Transformation ==="
python ingestion/silver_fingrid.py
python ingestion/silver_entsoe.py

echo "=== Starting Gold Layer Spine Join ==="
python ingestion/gold_power_balance_15min.py

echo "=== Running Automated Data Quality Tests ==="
pytest -v tests/test_gold_integrity.py

echo "=== Loading Verified Gold Data into PostgreSQL ==="
python ingestion/load_postgres.py