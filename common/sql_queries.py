from common.file_config import (
    dim_datetime_table_name,
    dim_fi_city_table_name,
    dim_fi_region_table_name,
    dim_fuel_type_table_name,
    dim_grid_zone_table_name,
    dim_power_unit_table_name,
    dim_weather_station_table_name,
    fct_fuel_generation_table_name,
    fct_power_market_table_name,
    fct_unit_generation_table_name,
    vw_fuel_generation_360_name,
    vw_regional_generation_summary_name,
    vw_unit_generation_360_name,
)

# SQL DDL for PostgreSQL Star Schema (Finland Power Generation Analytics)
CREATE_STAR_SCHEMA_DDL = f"""
CREATE TABLE IF NOT EXISTS {dim_datetime_table_name} (
    start_time_utc TIMESTAMP PRIMARY KEY,
    start_time_helsinki TIMESTAMP NOT NULL,
    date_start_helsinki DATE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    hour_helsinki INTEGER NOT NULL,
    season VARCHAR(16) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_peak_hour BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS {dim_fuel_type_table_name} (
    entsoe_psr_code VARCHAR(16) PRIMARY KEY,
    psr_type VARCHAR(64) NOT NULL,
    fuel_category VARCHAR(32) NOT NULL,
    source_category VARCHAR(32) NOT NULL,
    is_renewable BOOLEAN NOT NULL,
    is_dispatchable BOOLEAN NOT NULL,
    carbon_g_per_kwh DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS {dim_fi_region_table_name} (
    region VARCHAR(64) NOT NULL,
    region_fi VARCHAR(64) PRIMARY KEY,
    country VARCHAR(32) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS {dim_fi_city_table_name} (
    city VARCHAR(64) NOT NULL,
    city_ascii VARCHAR(64) PRIMARY KEY,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL,
    country VARCHAR(32) NOT NULL,
    iso2 VARCHAR(8) NOT NULL,
    iso3 VARCHAR(8) NOT NULL,
    admin_name VARCHAR(64) NOT NULL,
    capital VARCHAR(32) NOT NULL,
    population INTEGER NOT NULL,
    id BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS {dim_power_unit_table_name} (
    unit_mrid VARCHAR(32) PRIMARY KEY,
    unit_name VARCHAR(128) NOT NULL,
    entsoe_psr_code VARCHAR(16) NOT NULL REFERENCES {dim_fuel_type_table_name}(entsoe_psr_code),
    installed_capacity_mw DOUBLE PRECISION NOT NULL,
    city_ascii VARCHAR(64) NOT NULL,
    region_fi VARCHAR(64) NOT NULL,
    operator VARCHAR(128) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    is_active BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS {dim_weather_station_table_name} (
    station_id VARCHAR(32) PRIMARY KEY,
    city_name VARCHAR(64) NOT NULL,
    region VARCHAR(64) NOT NULL,
    grid_role VARCHAR(64) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS {dim_grid_zone_table_name} (
    grid_zone VARCHAR(32) PRIMARY KEY,
    zone_name VARCHAR(128) NOT NULL,
    fingrid_corridor VARCHAR(128) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    nominal_capacity_mw DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS {fct_unit_generation_table_name} (
    start_time_utc TIMESTAMP NOT NULL REFERENCES {dim_datetime_table_name}(start_time_utc),
    unit_mrid VARCHAR(32) NOT NULL REFERENCES {dim_power_unit_table_name}(unit_mrid),
    entsoe_psr_code VARCHAR(16) NOT NULL REFERENCES {dim_fuel_type_table_name}(entsoe_psr_code),
    generation_mw DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (start_time_utc, unit_mrid)
);

CREATE TABLE IF NOT EXISTS {fct_fuel_generation_table_name} (
    start_time_utc TIMESTAMP NOT NULL REFERENCES {dim_datetime_table_name}(start_time_utc),
    entsoe_psr_code VARCHAR(16) NOT NULL REFERENCES {dim_fuel_type_table_name}(entsoe_psr_code),
    generation_mw DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (start_time_utc, entsoe_psr_code)
);

CREATE TABLE IF NOT EXISTS {fct_power_market_table_name} (
    start_time_utc TIMESTAMP PRIMARY KEY REFERENCES {dim_datetime_table_name}(start_time_utc),
    wind_mw DOUBLE PRECISION NOT NULL,
    nuclear_mw DOUBLE PRECISION NOT NULL,
    hydro_mw DOUBLE PRECISION NOT NULL,
    solar_mw DOUBLE PRECISION NOT NULL,
    consumption_mw DOUBLE PRECISION NOT NULL,
    price_eur_per_mwh DOUBLE PRECISION NOT NULL,
    temp_c DOUBLE PRECISION NOT NULL,
    wind_speed_ms DOUBLE PRECISION NOT NULL,
    selected_generation_mw DOUBLE PRECISION NOT NULL,
    selected_gen_balance_mw DOUBLE PRECISION NOT NULL,
    wind_share_selected_ratio DOUBLE PRECISION NOT NULL
);
"""

# Idempotent Upsert statements (staging table -> target table)
UPSERT_DIM_DATETIME_SQL = f"""
INSERT INTO {dim_datetime_table_name} (
    start_time_utc, start_time_helsinki, date_start_helsinki,
    year, month, hour_helsinki, season, is_weekend, is_peak_hour
)
SELECT
    start_time_utc, start_time_helsinki, date_start_helsinki,
    year, month, hour_helsinki, season, is_weekend, is_peak_hour
FROM stg_dim_datetime
ON CONFLICT (start_time_utc) DO UPDATE SET
    start_time_helsinki = EXCLUDED.start_time_helsinki,
    date_start_helsinki = EXCLUDED.date_start_helsinki,
    year = EXCLUDED.year,
    month = EXCLUDED.month,
    hour_helsinki = EXCLUDED.hour_helsinki,
    season = EXCLUDED.season,
    is_weekend = EXCLUDED.is_weekend,
    is_peak_hour = EXCLUDED.is_peak_hour;
"""

UPSERT_FCT_UNIT_GENERATION_SQL = f"""
INSERT INTO {fct_unit_generation_table_name} (
    start_time_utc, unit_mrid, entsoe_psr_code, generation_mw
)
SELECT
    start_time_utc, unit_mrid, entsoe_psr_code, generation_mw
FROM stg_fct_unit_generation
ON CONFLICT (start_time_utc, unit_mrid) DO UPDATE SET
    entsoe_psr_code = EXCLUDED.entsoe_psr_code,
    generation_mw = EXCLUDED.generation_mw;
"""

UPSERT_FCT_FUEL_GENERATION_SQL = f"""
INSERT INTO {fct_fuel_generation_table_name} (
    start_time_utc, entsoe_psr_code, generation_mw
)
SELECT
    start_time_utc, entsoe_psr_code, generation_mw
FROM stg_fct_fuel_generation
ON CONFLICT (start_time_utc, entsoe_psr_code) DO UPDATE SET
    generation_mw = EXCLUDED.generation_mw;
"""

# Multi-dimensional 360 Analytical Views
CREATE_ANALYTICAL_VIEWS_SQL = f"""
-- 1. Unit-level 360 View: Fact x Power Unit x Fuel Type x City x Region x Datetime
CREATE OR REPLACE VIEW {vw_unit_generation_360_name} AS
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
    c.city_ascii,
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
    ROUND((f.generation_mw * 0.25)::numeric, 3) AS generation_mwh,
    CASE
        WHEN u.installed_capacity_mw > 0 THEN
            ROUND((f.generation_mw / u.installed_capacity_mw)::numeric, 4)
        ELSE 0.0
    END AS capacity_factor,
    ROUND((f.generation_mw * 0.25 * ft.carbon_g_per_kwh / 1000.0)::numeric, 3) AS est_co2_tonnes
FROM {fct_unit_generation_table_name} f
JOIN {dim_datetime_table_name} d
    ON f.start_time_utc = d.start_time_utc
JOIN {dim_power_unit_table_name} u
    ON f.unit_mrid = u.unit_mrid
JOIN {dim_fuel_type_table_name} ft
    ON f.entsoe_psr_code = ft.entsoe_psr_code
LEFT JOIN {dim_fi_city_table_name} c
    ON u.city_ascii = c.city_ascii
LEFT JOIN {dim_fi_region_table_name} r
    ON u.region_fi = r.region_fi
WHERE u.is_active = TRUE;

-- 2. National Fuel-Mix 360 View: Fact (A75) x Fuel Type x Datetime (zero hardcoded carbon factors)
CREATE OR REPLACE VIEW {vw_fuel_generation_360_name} AS
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
    ROUND((f.generation_mw * 0.25)::numeric, 3) AS generation_mwh,
    ROUND((f.generation_mw * 0.25 * ft.carbon_g_per_kwh / 1000.0)::numeric, 3) AS est_co2_tonnes
FROM {fct_fuel_generation_table_name} f
JOIN {dim_datetime_table_name} d
    ON f.start_time_utc = d.start_time_utc
JOIN {dim_fuel_type_table_name} ft
    ON f.entsoe_psr_code = ft.entsoe_psr_code;

-- 3. Regional Generation & Capacity Summary View with Window Function Ranking
CREATE OR REPLACE VIEW {vw_regional_generation_summary_name} AS
SELECT
    year,
    month,
    region_en,
    region_fi,
    city,
    fuel_category,
    source_category,
    COUNT(DISTINCT unit_mrid) AS active_unit_count,
    ROUND(SUM(generation_mwh)::numeric, 1) AS total_generation_mwh,
    ROUND(AVG(capacity_factor)::numeric, 4) AS avg_capacity_factor,
    ROUND(SUM(est_co2_tonnes)::numeric, 1) AS total_co2_tonnes,
    ROW_NUMBER() OVER (
        PARTITION BY year, month, region_fi
        ORDER BY SUM(generation_mwh) DESC
    ) AS regional_fuel_rank
FROM {vw_unit_generation_360_name}
GROUP BY year, month, region_en, region_fi, city, fuel_category, source_category;
"""
