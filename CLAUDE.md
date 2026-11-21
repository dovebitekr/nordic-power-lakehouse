# CLAUDE.md

Guidance for AI coding assistants working in this repository.

## Commands

### Environment Setup
- **Python**: `3.11` (Java `17` required locally for PySpark; bundled inside `Dockerfile`), **Terraform**: `>= 1.5`
- **Install dependencies**: `pip install -r requirements.txt`
- **Required `.env` variables** (see `.env.example`; never commit `.env`):
  - `ENTSOE_API_KEY`, `GCP_PROJECT_ID`, `GCS_BUCKET_NAME`, `BQ_DATASET_NAME`
  - `GOOGLE_APPLICATION_CREDENTIALS` (local CLI runs) and `GCP_CREDENTIALS_PATH` (Docker volume mount path)

### Pipeline Execution (Local — Writes to GCS / BigQuery)
> Only run these commands when explicitly requested by the user (see Agent Rules below).

```bash
# Load .env and run Bronze -> Silver -> pytest -> BigQuery Gold
# Defaults to INITIALISE_START_DATE=2025-11-01 .. INITIALISE_END_DATE=2025-11-04 (see common/file_config.py)
# Safe to rerun: BigQuery Fact loads use staging tables + SQL MERGE (idempotent, no duplicates)
set -a && source .env && set +a
./run_pipeline.sh

# Run individual ingestion / transformation steps
python ingestion/entsoe_generation.py --dataset all --start 2025-11-01 --end 2025-11-04
python ingestion/entsoe.py --start 2025-11-01 --end 2025-11-04
python ingestion/weather.py --start 2025-11-01 --end 2025-11-04
python ingestion/silver_entsoe_generation.py
python ingestion/silver_entsoe.py
python ingestion/silver_weather.py
python ingestion/load_bigquery.py
```

### Testing & CI Scope (Read-Only — Safe to Run Anytime)
```bash
# Run full test suite
pytest -v

# Run a single test by name
pytest -k test_silver_fuel_generation_integrity -v

# List discovered test names without running
pytest --collect-only -q
```
- **Runs in both GitHub Actions CI (`.github/workflows/ci.yml`) and local**: static Dimension CSV PK/FK checks, config loading, and BigQuery SQL template checks (`test_resources_*`, `test_common_config_and_bq_queries_valid`).
- **Runs locally only (auto-skipped in CI)**: Silver Parquet integrity and `A73` vs `A75` physical cross-checks (`test_silver_*`, `test_unit_vs_fuel_physical_consistency`) because `data/silver/` is gitignored. **CI passing does not mean Silver data passed validation**; always run `pytest -v` locally after Silver runs.

### Docker & Airflow
```bash
# Start Airflow 2.9 + PySpark container (UI: http://localhost:8080, airflow/airflow)
docker compose up -d --build
docker compose logs -f airflow
docker compose down
```

### Infrastructure (Terraform)
```bash
cd terraform
terraform init
terraform plan -var="project=YOUR_GCP_PROJECT_ID" -var="gcs_bucket_name=YOUR_BUCKET"
terraform apply -var="project=YOUR_GCP_PROJECT_ID" -var="gcs_bucket_name=YOUR_BUCKET"
```
- Note: the GCP project variable is named `project` (not `project_id`). `create_vm` defaults to `false` to stay within GCP Always Free Tier (`us-central1` GCS + `US` BigQuery); confirm with the user before setting `create_vm=true`.

## Agent Rules (Do / Don't)

- **Cloud write guard**: All commands under `Pipeline Execution` write to GCS and/or BigQuery, and `terraform apply` provisions cloud resources. Run them **only when the user explicitly asks** (billing and Always Free Tier limits apply). Read-only commands (`pytest`, `pytest --collect-only`, `terraform plan`) are safe anytime.
- **Secret guard**: Never read, print, or commit `.env` or GCP credential JSON files (`gcp-credentials*.json`, `service-account*.json`, ADC files).
- **API rate/lag guard**: Do not change `max_chunk_days` or `UNIT_PUBLICATION_LAG_DAYS` unless ENTSO-E API behavior changes and the user confirms.
- **Schema sync**: Any column or type change must be updated together in `common/schema.py` and `common/bq_queries.py` (and reflected in `tests/test_pipeline_integrity.py` if tested).
- **No hardcoded config**: Read bucket names, dataset names, endpoints, and chunk limits through `common/config.yaml` and `common/file_config.py`.

## Gotchas & Domain Rules

### Analytical View `INNER JOIN`s & `is_active` Filtering (`common/bq_queries.py`)
- `vw_unit_generation_360` uses `INNER JOIN` on `dim_datetime`, `dim_power_unit`, and `dim_fuel_type`; `vw_fuel_generation_360` uses `INNER JOIN` on `dim_datetime` and `dim_fuel_type`. Any Fact row with an unmapped `unit_mrid`, unmapped `entsoe_psr_code`, or `start_time_utc` outside `dim_datetime` is **silently dropped** from the views without error.
- `vw_unit_generation_360` also filters `WHERE u.is_active = TRUE`, which excludes retired power units from both `vw_unit_generation_360` and the downstream `vw_regional_generation_summary`.

### MW → MWh Conversion Assumption
- Fact tables (`fct_unit_generation_15min`, `fct_fuel_generation_15min`) store **`generation_mw` only**. `generation_mwh` is calculated in the analytical views as `ROUND(generation_mw * 0.25, 3)`, assuming uniform 15-minute intervals.

### Timezone & DST Handling
- All Fact tables store, join, `MERGE`, and partition strictly on **`start_time_utc`** (`PARTITION BY DATE(start_time_utc)`), preventing duplicate or missing local-hour collisions during `Europe/Helsinki` DST transitions.
- Local Helsinki calendar attributes (`start_time_helsinki`, `date_start_helsinki`, `hour_helsinki`) are computed in `dim_datetime` (the only table partitioned by `date_start_helsinki`, while still using `start_time_utc` as its `MERGE` key) and exposed via the analytical views in `common/bq_queries.py`.

### ENTSO-E API Constraints
- **`A73` 1-day query cap**: Requests > 1 day return `HTTP 400`. `common/config.yaml` enforces `max_chunk_days: 1` for `unit_generation` (`7` for `fuel_generation`).
- **`A73` 5-day publication lag**: `dags/etl_batch_ingest_data.py` pulls `D-7..D-5` for `A73` (`UNIT_PUBLICATION_LAG_DAYS = 5`) and `D-2..D` for `A75`, `A44`, and weather to avoid empty XML responses.

### Handling New PSR Fuel Codes (e.g., `B25` Energy Storage)
When ENTSO-E starts reporting an unmapped PSR code, `test_silver_fuel_generation_integrity` fails intentionally:
1. Add the new `entsoe_psr_code` row to `resources/psr_fuel_type_mapping.csv`.
2. Fill **every column** following existing rows (`psr_type`, `fuel_category`, `source_category`, `is_renewable`, `is_dispatchable`, `carbon_g_per_kwh`) — the analytical views read all of these columns.
3. Re-run `pytest -v` to verify referential integrity.
4. Re-run `python ingestion/load_bigquery.py` (after user confirmation) to refresh `dim_fuel_type`.

### Physical Cross-Check Tolerances (`tests/test_pipeline_integrity.py`)
- Multi-fuel CHP plants may be reported as `B05` (Coal) in `A73` and `B01` (Biomass) in `A75`, and nuclear meters have minor SCADA rounding differences. Tests assert `B14` Nuclear relative difference `< 1%` and total `SUM(A73) <= SUM(A75)`.

### Docker Entrypoint Override
- `docker-compose.yml` sets `entrypoint: ["/bin/bash", "-c"]` so the base Airflow image does not intercept the inline startup command (`db migrate`, `airflow users create`, `scheduler &`, `webserver`). GCP credentials mount from `${GCP_CREDENTIALS_PATH:-~/.config/gcloud/application_default_credentials.json}` to `/opt/airflow/.google/credentials/adc.json:ro`.

## Architecture & File Map

For full diagrams and data source details, see `README.md`. **If `README.md` and code ever disagree, treat the code as the source of truth.**

- `common/` — Single source of truth: `config.yaml` (API/GCP defaults), `file_config.py` (env overrides & paths), `schema.py` (PySpark/BQ schemas), `bq_queries.py` (DDL, `MERGE`, analytical views).
- `ingestion/` — Bronze API fetchers (`entsoe_generation.py`, `entsoe.py`, `weather.py`), Silver PySpark jobs (`silver_*.py`), and Gold BigQuery loader (`load_bigquery.py`).
- `dags/` — Two-track Airflow DAGs: `etl_initialise_data.py` (`@once`) and `etl_batch_ingest_data.py` (`0 6 * * *`).
- `resources/` — Curated static Dimension CSVs (units, PSR fuel mappings, Finnish cities, Finnish regions).
- `tests/` — Write-Audit-Publish integrity suite (`test_pipeline_integrity.py`).
- `terraform/` — GCP IaC (`main.tf`, `variables.tf`); optional GCE VM uses root `start_up_script.sh`.
- `data/` — Local Bronze/Silver staging (gitignored).
