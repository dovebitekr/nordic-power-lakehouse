"""
Data ingestion, processing, saving, and loading configurations for GCP (GCS + BigQuery).
"""
from datetime import datetime
import os
from pathlib import Path
from dateutil.relativedelta import relativedelta
import yaml

BASE_DIR = Path(__file__).resolve().parent.parent

# Load the central YAML configuration file
with open(BASE_DIR / "common/config.yaml", "r", encoding="utf-8") as cfg_file:
    config = yaml.safe_load(cfg_file)

# Google Cloud Platform runtime configuration (env vars override YAML defaults)
GCP_CFG = config["gcp"]
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", GCP_CFG["project_id"])
GCP_REGION = os.getenv("GCP_REGION", GCP_CFG["region"])
BQ_LOCATION = os.getenv("BQ_LOCATION", GCP_CFG["bq_location"])
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", GCP_CFG["gcs_bucket"])
BQ_DATASET_NAME = os.getenv("BQ_DATASET_NAME", GCP_CFG["bq_dataset"])
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", str(BASE_DIR))

# Initialise backfill window vs incremental batch window
INITIALISE_START_DATE = os.getenv("INITIALISE_START_DATE", "2025-11-01")
INITIALISE_END_DATE = os.getenv("INITIALISE_END_DATE", "2026-11-01")
BATCH_LOOKBACK_DAYS = int(os.getenv("BATCH_LOOKBACK_DAYS", "2"))
UNIT_PUBLICATION_LAG_DAYS = int(os.getenv("UNIT_PUBLICATION_LAG_DAYS", "5"))

current_datetime = datetime.utcnow()
batch_from_date = (current_datetime - relativedelta(days=BATCH_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
batch_to_date = current_datetime.strftime("%Y-%m-%d")

# Local directory hierarchy (staging before GCS upload)
RESOURCES_DIR = BASE_DIR / "resources"
BRONZE_DIR = BASE_DIR / "data/bronze"
SILVER_DIR = BASE_DIR / "data/silver"

# Reference resource CSV file paths
PSR_FUEL_TYPE_MAPPING_FILE = RESOURCES_DIR / "psr_fuel_type_mapping.csv"
POWER_UNITS_MASTER_FILE = RESOURCES_DIR / "fi_power_units_master.csv"
FI_CITIES_COORDINATES_FILE = RESOURCES_DIR / "fi_cities_coordinates.csv"
FI_REGIONS_COORDINATES_FILE = RESOURCES_DIR / "fi_regions_coordinates.csv"

# Coordinate standardisation dictionary from YAML
coordinate_df_replace_mapping = config["coordinate_df_replace_mapping"]

# BigQuery table and view names from YAML
BQ_TABLES = config["bq_tables"]
dim_datetime_table_name = BQ_TABLES["dim_datetime"]
dim_fuel_type_table_name = BQ_TABLES["dim_fuel_type"]
dim_power_unit_table_name = BQ_TABLES["dim_power_unit"]
dim_fi_city_table_name = BQ_TABLES["dim_fi_city"]
dim_fi_region_table_name = BQ_TABLES["dim_fi_region"]
fct_unit_generation_table_name = BQ_TABLES["fct_unit_generation"]
fct_fuel_generation_table_name = BQ_TABLES["fct_fuel_generation"]
fct_day_ahead_price_table_name = BQ_TABLES["fct_day_ahead_price"]
fct_weather_table_name = BQ_TABLES["fct_weather"]
vw_unit_generation_360_name = BQ_TABLES["vw_unit_generation_360"]
vw_fuel_generation_360_name = BQ_TABLES["vw_fuel_generation_360"]
vw_regional_generation_summary_name = BQ_TABLES["vw_regional_generation_summary"]

# Configuration list for seeding reference dimension tables into GCS & BigQuery
initialise_dimension_files_config_list = [
    {
        "table_name": dim_fuel_type_table_name,
        "local_file": PSR_FUEL_TYPE_MAPPING_FILE,
        "gcs_path": f"resources/{PSR_FUEL_TYPE_MAPPING_FILE.name}",
        "pk": "entsoe_psr_code",
    },
    {
        "table_name": dim_fi_region_table_name,
        "local_file": FI_REGIONS_COORDINATES_FILE,
        "gcs_path": f"resources/{FI_REGIONS_COORDINATES_FILE.name}",
        "pk": "region_fi",
    },
    {
        "table_name": dim_fi_city_table_name,
        "local_file": FI_CITIES_COORDINATES_FILE,
        "gcs_path": f"resources/{FI_CITIES_COORDINATES_FILE.name}",
        "pk": "city_ascii",
    },
    {
        "table_name": dim_power_unit_table_name,
        "local_file": POWER_UNITS_MASTER_FILE,
        "gcs_path": f"resources/{POWER_UNITS_MASTER_FILE.name}",
        "pk": "unit_mrid",
    },
]
