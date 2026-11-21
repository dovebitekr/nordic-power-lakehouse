"""
Upload static reference CSVs to GCS, load Dimension & Fact tables into Google BigQuery
using idempotent MERGE upserts, and create multi-dimensional 360 analytical views.
"""
import logging
from pathlib import Path
import sys
from zoneinfo import ZoneInfo
from google.cloud import bigquery, storage
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from common.bq_queries import (
    CREATE_ANALYTICAL_VIEWS_SQL,
    CREATE_FACT_TABLES_SQL,
    MERGE_DIM_DATETIME_SQL,
    MERGE_FCT_DAY_AHEAD_PRICE_SQL,
    MERGE_FCT_FUEL_GENERATION_SQL,
    MERGE_FCT_UNIT_GENERATION_SQL,
    MERGE_FCT_WEATHER_SQL,
    bq_table,
)
from common.file_config import (
    BQ_DATASET_NAME,
    BQ_LOCATION,
    GCP_PROJECT_ID,
    GCS_BUCKET_NAME,
    SILVER_DIR,
    config,
    initialise_dimension_files_config_list,
    vw_fuel_generation_360_name,
    vw_unit_generation_360_name,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

HELSINKI_TZ = ZoneInfo("Europe/Helsinki")


def get_season(month: int) -> str:
    if month in (12, 1, 2):
        return "Winter"
    if month in (3, 4, 5):
        return "Spring"
    if month in (6, 7, 8):
        return "Summer"
    return "Autumn"


def load_dimensions_via_gcs(bq_client: bigquery.Client, gcs_bucket) -> None:
    """Upload reference CSVs to GCS and load into BigQuery dimension tables."""
    for item in initialise_dimension_files_config_list:
        local_file: Path = item["local_file"]
        gcs_path: str = item["gcs_path"]
        table_name: str = item["table_name"]

        gcs_bucket.blob(gcs_path).upload_from_filename(str(local_file))
        gcs_uri = f"gs://{gcs_bucket.name}/{gcs_path}"
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config, location=BQ_LOCATION)
        load_job.result()
        dest_table = bq_client.get_table(table_id)
        logging.info("Loaded dimension %s from %s (%s rows)", table_name, gcs_uri, dest_table.num_rows)


def build_dim_datetime_df(utc_series_list: list[pd.Series]) -> pd.DataFrame:
    """Build dim_datetime DataFrame from all unique UTC timestamps across Silver facts."""
    all_ts = pd.concat(utc_series_list, ignore_index=True).dropna().drop_duplicates().sort_values()
    utc_dt = pd.to_datetime(all_ts, utc=True)
    hel_dt = utc_dt.dt.tz_convert(HELSINKI_TZ)

    return pd.DataFrame({
        "start_time_utc": utc_dt,
        "start_time_helsinki": hel_dt.dt.tz_localize(None),
        "date_start_helsinki": hel_dt.dt.date,
        "year": hel_dt.dt.year.astype("int64"),
        "month": hel_dt.dt.month.astype("int64"),
        "hour_helsinki": hel_dt.dt.hour.astype("int64"),
        "season": hel_dt.dt.month.map(get_season),
        "is_weekend": hel_dt.dt.day_of_week.isin([5, 6]),
        "is_peak_hour": (~hel_dt.dt.day_of_week.isin([5, 6])) & (hel_dt.dt.hour >= 8) & (hel_dt.dt.hour < 20),
    })


def stage_and_merge(
    bq_client: bigquery.Client,
    df: pd.DataFrame,
    staging_table_name: str,
    merge_sql: str,
    schema: list[bigquery.SchemaField] | None = None,
) -> None:
    """Load DataFrame to a BigQuery staging table, execute MERGE upsert, and drop staging."""
    stg_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{staging_table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )
    bq_client.load_table_from_dataframe(df, stg_table_id, job_config=job_config, location=BQ_LOCATION).result()
    bq_client.query(merge_sql, location=BQ_LOCATION).result()
    bq_client.delete_table(stg_table_id, not_found_ok=True)
    logging.info("Merged %s rows via %s", len(df), staging_table_name)


def main() -> None:
    bq_client = bigquery.Client(project=GCP_PROJECT_ID, location=BQ_LOCATION)
    gcs_client = storage.Client(project=GCP_PROJECT_ID)
    gcs_bucket = gcs_client.bucket(GCS_BUCKET_NAME)

    # Seed dimension tables from resources via GCS
    load_dimensions_via_gcs(bq_client, gcs_bucket)

    # Ensure partitioned and clustered fact tables exist
    for ddl in CREATE_FACT_TABLES_SQL:
        bq_client.query(ddl, location=BQ_LOCATION).result()
    logging.info("Verified %s partitioned/clustered BigQuery tables", len(CREATE_FACT_TABLES_SQL))

    # Read available Silver Parquet tables
    fact_jobs = [
        (
            SILVER_DIR / "entsoe" / config["entsoe"]["datasets"]["unit_generation"]["table_name"],
            ["start_time_utc", "unit_mrid", "entsoe_psr_code", "generation_mw"],
            "stg_fct_unit_generation",
            MERGE_FCT_UNIT_GENERATION_SQL,
        ),
        (
            SILVER_DIR / "entsoe" / config["entsoe"]["datasets"]["fuel_generation"]["table_name"],
            ["start_time_utc", "entsoe_psr_code", "generation_mw"],
            "stg_fct_fuel_generation",
            MERGE_FCT_FUEL_GENERATION_SQL,
        ),
        (
            SILVER_DIR / "entsoe" / config["entsoe"]["table_name"],
            ["start_time_utc", "price_eur_per_mwh", "source_resolution"],
            "stg_fct_day_ahead_price",
            MERGE_FCT_DAY_AHEAD_PRICE_SQL,
        ),
        (
            SILVER_DIR / "weather" / config["weather"]["table_name"],
            ["start_time_utc", "temp_c", "wind_speed_ms"],
            "stg_fct_weather",
            MERGE_FCT_WEATHER_SQL,
        ),
    ]

    loaded_dfs = []
    utc_series_list = []
    for path, cols, stg_name, merge_sql in fact_jobs:
        if not path.exists():
            logging.warning("Skipping missing Silver Parquet directory: %s", path)
            continue
        df = pd.read_parquet(path)[cols].copy()
        df["start_time_utc"] = pd.to_datetime(df["start_time_utc"], utc=True)
        loaded_dfs.append((df, stg_name, merge_sql))
        utc_series_list.append(df["start_time_utc"])

    if not utc_series_list:
        logging.error("No Silver Parquet datasets found to load into BigQuery")
        sys.exit(1)

    # Merge dim_datetime first, then merge all fact tables
    dim_dt_df = build_dim_datetime_df(utc_series_list)
    dim_dt_schema = [
        bigquery.SchemaField("start_time_utc", "TIMESTAMP"),
        bigquery.SchemaField("start_time_helsinki", "DATETIME"),
        bigquery.SchemaField("date_start_helsinki", "DATE"),
        bigquery.SchemaField("year", "INT64"),
        bigquery.SchemaField("month", "INT64"),
        bigquery.SchemaField("hour_helsinki", "INT64"),
        bigquery.SchemaField("season", "STRING"),
        bigquery.SchemaField("is_weekend", "BOOL"),
        bigquery.SchemaField("is_peak_hour", "BOOL"),
    ]
    stage_and_merge(bq_client, dim_dt_df, "stg_dim_datetime", MERGE_DIM_DATETIME_SQL, schema=dim_dt_schema)

    for df, stg_name, merge_sql in loaded_dfs:
        stage_and_merge(bq_client, df, stg_name, merge_sql)

    # Create multi-dimensional 360 analytical views
    for view_sql in CREATE_ANALYTICAL_VIEWS_SQL:
        bq_client.query(view_sql, location=BQ_LOCATION).result()
    logging.info("Created %s BigQuery 360 analytical views", len(CREATE_ANALYTICAL_VIEWS_SQL))

    # Post-load verification query on 360 views
    verify_sql = f"""
    SELECT
        unit_name,
        city,
        region_en,
        fuel_category,
        ROUND(AVG(generation_mw), 1) AS avg_mw,
        ROUND(AVG(capacity_factor), 3) AS avg_cf,
        ROUND(SUM(est_co2_tonnes), 1) AS total_co2_t
    FROM {bq_table(vw_unit_generation_360_name)}
    GROUP BY unit_name, city, region_en, fuel_category
    ORDER BY avg_mw DESC
    LIMIT 5
    """
    rows = list(bq_client.query(verify_sql, location=BQ_LOCATION).result())
    logging.info("Top 5 Finnish power units in BigQuery vw_unit_generation_360:")
    for r in rows:
        logging.info(
            "  %-22s | %-12s | %-16s | %-10s | avg=%6.1f MW | CF=%.3f | CO2=%.1f t",
            r.unit_name, r.city, r.region_en, r.fuel_category, r.avg_mw, r.avg_cf, r.total_co2_t,
        )

    fuel_sql = f"""
    SELECT fuel_category, source_category, ROUND(AVG(generation_mw), 1) AS avg_mw
    FROM {bq_table(vw_fuel_generation_360_name)}
    GROUP BY fuel_category, source_category
    ORDER BY avg_mw DESC
    LIMIT 5
    """
    fuel_rows = list(bq_client.query(fuel_sql, location=BQ_LOCATION).result())
    logging.info("Top 5 Finnish fuel categories in BigQuery vw_fuel_generation_360:")
    for r in fuel_rows:
        logging.info("  %-16s | %-8s | avg=%6.1f MW", r.fuel_category, r.source_category, r.avg_mw)


if __name__ == "__main__":
    main()
