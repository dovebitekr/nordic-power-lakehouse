"""
Data ingestion, processing, saving, and loading configurations.
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

# Environment variables for runtime and database connection
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", str(BASE_DIR))
POSTGRES_USER = os.getenv("POSTGRES_USER", "lakehouse")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "lakehouse_pass")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5433")
POSTGRES_DB = os.getenv("POSTGRES_DB", "nordic_power")

# Initialise backfill window vs incremental batch window
INITIALISE_START_DATE = os.getenv("INITIALISE_START_DATE", "2025-11-01")
INITIALISE_END_DATE = os.getenv("INITIALISE_END_DATE", "2026-11-01")
BATCH_LOOKBACK_DAYS = int(os.getenv("BATCH_LOOKBACK_DAYS", "2"))

current_datetime = datetime.utcnow()
batch_from_date = (current_datetime - relativedelta(days=BATCH_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
batch_to_date = current_datetime.strftime("%Y-%m-%d")

# Local directory hierarchy
RESOURCES_DIR = BASE_DIR / "resources"
BRONZE_DIR = BASE_DIR / "data/bronze"
SILVER_DIR = BASE_DIR / "data/silver"
GOLD_DIR = BASE_DIR / "data/gold/power_market_15min"
BESS_DAILY_CSV = BASE_DIR / "data/gold/bess_daily_simulation.csv"

# Reference resource CSV file paths
FUEL_TYPE_MAPPING_FILE = RESOURCES_DIR / "fuel_type_mapping.csv"
WEATHER_STATIONS_FILE = RESOURCES_DIR / "fi_weather_stations.csv"
POWER_UNITS_OVERLAY_FILE = RESOURCES_DIR / "fi_power_units_overlay.csv"
FI_CITIES_COORDINATES_FILE = RESOURCES_DIR / "fi_cities_coordinates.csv"
FI_REGIONS_COORDINATES_FILE = RESOURCES_DIR / "fi_regions_coordinates.csv"
FI_GRID_ZONES_COORDINATES_FILE = RESOURCES_DIR / "fi_grid_zones_coordinates.csv"

# Coordinate standardisation dictionary from YAML
coordinate_df_replace_mapping = config["coordinate_df_replace_mapping"]

# PostgreSQL table and view names from YAML
PG_TABLES = config["postgres_tables"]
dim_datetime_table_name = PG_TABLES["dim_datetime"]
dim_energy_source_table_name = PG_TABLES["dim_energy_source"]
dim_weather_station_table_name = PG_TABLES["dim_weather_station"]
dim_power_unit_table_name = PG_TABLES["dim_power_unit"]
dim_fi_city_table_name = PG_TABLES["dim_fi_city"]
dim_fi_region_table_name = PG_TABLES["dim_fi_region"]
dim_grid_zone_table_name = PG_TABLES["dim_grid_zone"]
fct_power_market_table_name = PG_TABLES["fct_power_market"]
fct_bess_daily_table_name = PG_TABLES["fct_bess_daily"]
vw_power_market_360_name = PG_TABLES["vw_power_market_360"]
vw_monthly_market_summary_name = PG_TABLES["vw_monthly_market_summary"]
vw_regional_capacity_by_fuel_name = PG_TABLES["vw_regional_capacity_by_fuel"]

# Configuration list for seeding reference dimension tables during initialisation
initialise_dimension_files_config_list = [
    {
        "table_name": dim_energy_source_table_name,
        "local_file": FUEL_TYPE_MAPPING_FILE,
        "pk": "source_key",
    },
    {
        "table_name": dim_weather_station_table_name,
        "local_file": WEATHER_STATIONS_FILE,
        "pk": "station_id",
    },
    {
        "table_name": dim_power_unit_table_name,
        "local_file": POWER_UNITS_OVERLAY_FILE,
        "pk": "unit_id",
    },
    {
        "table_name": dim_fi_city_table_name,
        "local_file": FI_CITIES_COORDINATES_FILE,
        "pk": "city",
    },
    {
        "table_name": dim_fi_region_table_name,
        "local_file": FI_REGIONS_COORDINATES_FILE,
        "pk": "region",
    },
    {
        "table_name": dim_grid_zone_table_name,
        "local_file": FI_GRID_ZONES_COORDINATES_FILE,
        "pk": "grid_zone",
    },
]
