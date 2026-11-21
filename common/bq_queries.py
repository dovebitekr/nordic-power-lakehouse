"""
BigQuery Standard SQL DDL, Partitioned/Clustered Tables, MERGE Upserts,
and Multi-Dimensional 360 Analytical Views.
"""
from common.file_config import (
    BQ_DATASET_NAME,
    GCP_PROJECT_ID,
    dim_datetime_table_name,
    dim_fi_city_table_name,
    dim_fi_region_table_name,
    dim_fuel_type_table_name,
    dim_power_unit_table_name,
    fct_day_ahead_price_table_name,
    fct_fuel_generation_table_name,
    fct_unit_generation_table_name,
    fct_weather_table_name,
    vw_fuel_generation_360_name,
    vw_regional_generation_summary_name,
    vw_unit_generation_360_name,
)


def bq_table(table_name: str) -> str:
    return f"`{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{table_name}`"


# Partitioned and clustered Fact/Time table DDL
CREATE_FACT_TABLES_SQL = [
    f"""
    CREATE TABLE IF NOT EXISTS {bq_table(dim_datetime_table_name)} (
        start_time_utc TIMESTAMP NOT NULL,
        start_time_helsinki DATETIME NOT NULL,
        date_start_helsinki DATE NOT NULL,
        year INT64 NOT NULL,
        month INT64 NOT NULL,
        hour_helsinki INT64 NOT NULL,
        season STRING NOT NULL,
        is_weekend BOOL NOT NULL,
        is_peak_hour BOOL NOT NULL
    )
    PARTITION BY date_start_helsinki
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {bq_table(fct_unit_generation_table_name)} (
        start_time_utc TIMESTAMP NOT NULL,
        unit_mrid STRING NOT NULL,
        entsoe_psr_code STRING NOT NULL,
        generation_mw FLOAT64 NOT NULL
    )
    PARTITION BY DATE(start_time_utc)
    CLUSTER BY unit_mrid, entsoe_psr_code
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {bq_table(fct_fuel_generation_table_name)} (
        start_time_utc TIMESTAMP NOT NULL,
        entsoe_psr_code STRING NOT NULL,
        generation_mw FLOAT64 NOT NULL
    )
    PARTITION BY DATE(start_time_utc)
    CLUSTER BY entsoe_psr_code
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {bq_table(fct_day_ahead_price_table_name)} (
        start_time_utc TIMESTAMP NOT NULL,
        price_eur_per_mwh FLOAT64 NOT NULL,
        source_resolution STRING
    )
    PARTITION BY DATE(start_time_utc)
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {bq_table(fct_weather_table_name)} (
        start_time_utc TIMESTAMP NOT NULL,
        temp_c FLOAT64 NOT NULL,
        wind_speed_ms FLOAT64 NOT NULL
    )
    PARTITION BY DATE(start_time_utc)
    """,
]


# Idempotent MERGE upsert queries
MERGE_DIM_DATETIME_SQL = f"""
MERGE {bq_table(dim_datetime_table_name)} T
USING {bq_table("stg_dim_datetime")} S
ON T.start_time_utc = S.start_time_utc
WHEN MATCHED THEN
    UPDATE SET
        start_time_helsinki = S.start_time_helsinki,
        date_start_helsinki = S.date_start_helsinki,
        year = S.year,
        month = S.month,
        hour_helsinki = S.hour_helsinki,
        season = S.season,
        is_weekend = S.is_weekend,
        is_peak_hour = S.is_peak_hour
WHEN NOT MATCHED THEN
    INSERT (
        start_time_utc, start_time_helsinki, date_start_helsinki,
        year, month, hour_helsinki, season, is_weekend, is_peak_hour
    )
    VALUES (
        S.start_time_utc, S.start_time_helsinki, S.date_start_helsinki,
        S.year, S.month, S.hour_helsinki, S.season, S.is_weekend, S.is_peak_hour
    );
"""

MERGE_FCT_UNIT_GENERATION_SQL = f"""
MERGE {bq_table(fct_unit_generation_table_name)} T
USING {bq_table("stg_fct_unit_generation")} S
ON T.start_time_utc = S.start_time_utc
   AND T.unit_mrid = S.unit_mrid
WHEN MATCHED THEN
    UPDATE SET
        entsoe_psr_code = S.entsoe_psr_code,
        generation_mw = S.generation_mw
WHEN NOT MATCHED THEN
    INSERT (start_time_utc, unit_mrid, entsoe_psr_code, generation_mw)
    VALUES (S.start_time_utc, S.unit_mrid, S.entsoe_psr_code, S.generation_mw);
"""

MERGE_FCT_FUEL_GENERATION_SQL = f"""
MERGE {bq_table(fct_fuel_generation_table_name)} T
USING {bq_table("stg_fct_fuel_generation")} S
ON T.start_time_utc = S.start_time_utc
   AND T.entsoe_psr_code = S.entsoe_psr_code
WHEN MATCHED THEN
    UPDATE SET
        generation_mw = S.generation_mw
WHEN NOT MATCHED THEN
    INSERT (start_time_utc, entsoe_psr_code, generation_mw)
    VALUES (S.start_time_utc, S.entsoe_psr_code, S.generation_mw);
"""

MERGE_FCT_DAY_AHEAD_PRICE_SQL = f"""
MERGE {bq_table(fct_day_ahead_price_table_name)} T
USING {bq_table("stg_fct_day_ahead_price")} S
ON T.start_time_utc = S.start_time_utc
WHEN MATCHED THEN
    UPDATE SET
        price_eur_per_mwh = S.price_eur_per_mwh,
        source_resolution = S.source_resolution
WHEN NOT MATCHED THEN
    INSERT (start_time_utc, price_eur_per_mwh, source_resolution)
    VALUES (S.start_time_utc, S.price_eur_per_mwh, S.source_resolution);
"""

MERGE_FCT_WEATHER_SQL = f"""
MERGE {bq_table(fct_weather_table_name)} T
USING {bq_table("stg_fct_weather")} S
ON T.start_time_utc = S.start_time_utc
WHEN MATCHED THEN
    UPDATE SET
        temp_c = S.temp_c,
        wind_speed_ms = S.wind_speed_ms
WHEN NOT MATCHED THEN
    INSERT (start_time_utc, temp_c, wind_speed_ms)
    VALUES (S.start_time_utc, S.temp_c, S.wind_speed_ms);
"""


# Multi-dimensional 360 analytical views
CREATE_ANALYTICAL_VIEWS_SQL = [
    # Unit-level 360 view
    f"""
    CREATE OR REPLACE VIEW {bq_table(vw_unit_generation_360_name)} AS
    SELECT
        f.start_time_utc,
        d.start_time_helsinki,
        d.date_start_helsinki,
        d.year,
        d.month,
        d.hour_helsinki,
        d.season,
        d.is_weekend,
        d.is_peak_hour,
        f.unit_mrid,
        u.unit_name,
        u.operator,
        u.installed_capacity_mw,
        u.latitude AS unit_latitude,
        u.longitude AS unit_longitude,
        c.city,
        r.region AS region_en,
        r.region_fi,
        ft.entsoe_psr_code,
        ft.psr_type,
        ft.fuel_category,
        ft.source_category,
        ft.is_renewable,
        ft.is_dispatchable,
        ft.carbon_g_per_kwh,
        f.generation_mw,
        ROUND(f.generation_mw * 0.25, 3) AS generation_mwh,
        ROUND(SAFE_DIVIDE(f.generation_mw, u.installed_capacity_mw), 4) AS capacity_factor,
        ROUND(f.generation_mw * 0.25 * ft.carbon_g_per_kwh / 1000.0, 3) AS est_co2_tonnes,
        p.price_eur_per_mwh,
        w.temp_c,
        w.wind_speed_ms
    FROM {bq_table(fct_unit_generation_table_name)} f
    JOIN {bq_table(dim_datetime_table_name)} d
        ON f.start_time_utc = d.start_time_utc
    JOIN {bq_table(dim_power_unit_table_name)} u
        ON f.unit_mrid = u.unit_mrid
    JOIN {bq_table(dim_fuel_type_table_name)} ft
        ON f.entsoe_psr_code = ft.entsoe_psr_code
    LEFT JOIN {bq_table(dim_fi_city_table_name)} c
        ON u.city_ascii = c.city_ascii
    LEFT JOIN {bq_table(dim_fi_region_table_name)} r
        ON u.region_fi = r.region_fi
    LEFT JOIN {bq_table(fct_day_ahead_price_table_name)} p
        ON f.start_time_utc = p.start_time_utc
    LEFT JOIN {bq_table(fct_weather_table_name)} w
        ON f.start_time_utc = w.start_time_utc
    WHERE u.is_active = TRUE
    """,
    # National fuel-mix 360 view
    f"""
    CREATE OR REPLACE VIEW {bq_table(vw_fuel_generation_360_name)} AS
    SELECT
        f.start_time_utc,
        d.start_time_helsinki,
        d.date_start_helsinki,
        d.year,
        d.month,
        d.hour_helsinki,
        d.season,
        d.is_weekend,
        d.is_peak_hour,
        ft.entsoe_psr_code,
        ft.psr_type,
        ft.fuel_category,
        ft.source_category,
        ft.is_renewable,
        ft.is_dispatchable,
        ft.carbon_g_per_kwh,
        f.generation_mw,
        ROUND(f.generation_mw * 0.25, 3) AS generation_mwh,
        ROUND(f.generation_mw * 0.25 * ft.carbon_g_per_kwh / 1000.0, 3) AS est_co2_tonnes,
        p.price_eur_per_mwh,
        w.temp_c,
        w.wind_speed_ms
    FROM {bq_table(fct_fuel_generation_table_name)} f
    JOIN {bq_table(dim_datetime_table_name)} d
        ON f.start_time_utc = d.start_time_utc
    JOIN {bq_table(dim_fuel_type_table_name)} ft
        ON f.entsoe_psr_code = ft.entsoe_psr_code
    LEFT JOIN {bq_table(fct_day_ahead_price_table_name)} p
        ON f.start_time_utc = p.start_time_utc
    LEFT JOIN {bq_table(fct_weather_table_name)} w
        ON f.start_time_utc = w.start_time_utc
    """,
    # Regional generation summary view with window function ranking
    f"""
    CREATE OR REPLACE VIEW {bq_table(vw_regional_generation_summary_name)} AS
    SELECT
        year,
        month,
        region_en,
        region_fi,
        city,
        fuel_category,
        source_category,
        COUNT(DISTINCT unit_mrid) AS active_unit_count,
        ROUND(SUM(generation_mwh), 1) AS total_generation_mwh,
        ROUND(AVG(capacity_factor), 4) AS avg_capacity_factor,
        ROUND(SUM(est_co2_tonnes), 1) AS total_co2_tonnes,
        ROUND(AVG(price_eur_per_mwh), 2) AS avg_price_eur_mwh,
        ROW_NUMBER() OVER (
            PARTITION BY year, month, region_fi
            ORDER BY SUM(generation_mwh) DESC
        ) AS regional_fuel_rank
    FROM {bq_table(vw_unit_generation_360_name)}
    GROUP BY year, month, region_en, region_fi, city, fuel_category, source_category
    """,
]
