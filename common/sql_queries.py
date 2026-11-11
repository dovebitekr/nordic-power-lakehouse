from common.file_config import (
    dim_datetime_table_name,
    dim_energy_source_table_name,
    dim_fi_city_table_name,
    dim_fi_region_table_name,
    dim_grid_zone_table_name,
    dim_power_unit_table_name,
    dim_weather_station_table_name,
    fct_bess_daily_table_name,
    fct_power_market_table_name,
    vw_monthly_market_summary_name,
    vw_power_market_360_name,
    vw_regional_capacity_by_fuel_name,
)

# SQL DDL, Upsert, and Analytical View statements for PostgreSQL Star Schema
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

CREATE TABLE IF NOT EXISTS {dim_energy_source_table_name} (
    source_key VARCHAR(32) PRIMARY KEY,
    entsoe_psr_code VARCHAR(16) NOT NULL,
    psr_type VARCHAR(64) NOT NULL,
    fingrid_dataset_id INTEGER NOT NULL,
    column_name VARCHAR(64) NOT NULL,
    fuel_category VARCHAR(32) NOT NULL,
    source_category VARCHAR(32) NOT NULL,
    measurement_type VARCHAR(32) NOT NULL,
    is_renewable BOOLEAN NOT NULL,
    is_dispatchable BOOLEAN NOT NULL,
    carbon_g_per_kwh DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS {dim_weather_station_table_name} (
    station_id VARCHAR(32) PRIMARY KEY,
    city_name VARCHAR(64) NOT NULL,
    region VARCHAR(64) NOT NULL,
    grid_role VARCHAR(64) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS {dim_power_unit_table_name} (
    unit_id VARCHAR(32) PRIMARY KEY,
    unit_name VARCHAR(128) NOT NULL,
    psr_type VARCHAR(64) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    capacity_mw DOUBLE PRECISION NOT NULL,
    municipality VARCHAR(64) NOT NULL,
    operator VARCHAR(128) NOT NULL
);

CREATE TABLE IF NOT EXISTS {dim_fi_city_table_name} (
    city VARCHAR(64) PRIMARY KEY,
    city_ascii VARCHAR(64) NOT NULL,
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

CREATE TABLE IF NOT EXISTS {dim_fi_region_table_name} (
    region VARCHAR(64) PRIMARY KEY,
    region_fi VARCHAR(64) NOT NULL,
    country VARCHAR(32) NOT NULL,
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

CREATE TABLE IF NOT EXISTS {fct_bess_daily_table_name} (
    trade_date_cet DATE PRIMARY KEY,
    buy_threshold_eur DOUBLE PRECISION NOT NULL,
    sell_threshold_eur DOUBLE PRECISION NOT NULL,
    is_profitable_day BOOLEAN NOT NULL,
    charge_steps INTEGER NOT NULL,
    discharge_steps INTEGER NOT NULL,
    rule_revenue_eur DOUBLE PRECISION NOT NULL,
    avg_temp_c DOUBLE PRECISION NOT NULL,
    avg_wind_speed_ms DOUBLE PRECISION NOT NULL,
    avg_price_eur_mwh DOUBLE PRECISION NOT NULL
);
"""

UPSERT_DIM_DATETIME_SQL = f"""
INSERT INTO {dim_datetime_table_name} (
    start_time_utc,
    start_time_helsinki,
    date_start_helsinki,
    year,
    month,
    hour_helsinki,
    season,
    is_weekend,
    is_peak_hour
)
SELECT
    start_time_utc,
    start_time_helsinki,
    date_start_helsinki,
    year,
    month,
    hour_helsinki,
    season,
    is_weekend,
    is_peak_hour
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

UPSERT_FCT_POWER_MARKET_SQL = f"""
INSERT INTO {fct_power_market_table_name} (
    start_time_utc,
    wind_mw,
    nuclear_mw,
    hydro_mw,
    solar_mw,
    consumption_mw,
    price_eur_per_mwh,
    temp_c,
    wind_speed_ms,
    selected_generation_mw,
    selected_gen_balance_mw,
    wind_share_selected_ratio
)
SELECT
    start_time_utc,
    wind_mw,
    nuclear_mw,
    hydro_mw,
    solar_mw,
    consumption_mw,
    price_eur_per_mwh,
    temp_c,
    wind_speed_ms,
    selected_generation_mw,
    selected_gen_balance_mw,
    wind_share_selected_ratio
FROM stg_fct_power_market
ON CONFLICT (start_time_utc) DO UPDATE SET
    wind_mw = EXCLUDED.wind_mw,
    nuclear_mw = EXCLUDED.nuclear_mw,
    hydro_mw = EXCLUDED.hydro_mw,
    solar_mw = EXCLUDED.solar_mw,
    consumption_mw = EXCLUDED.consumption_mw,
    price_eur_per_mwh = EXCLUDED.price_eur_per_mwh,
    temp_c = EXCLUDED.temp_c,
    wind_speed_ms = EXCLUDED.wind_speed_ms,
    selected_generation_mw = EXCLUDED.selected_generation_mw,
    selected_gen_balance_mw = EXCLUDED.selected_gen_balance_mw,
    wind_share_selected_ratio = EXCLUDED.wind_share_selected_ratio;
"""

CREATE_ANALYTICAL_VIEWS_SQL = f"""
CREATE OR REPLACE VIEW {vw_power_market_360_name} AS
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
    f.wind_mw,
    f.nuclear_mw,
    f.hydro_mw,
    f.solar_mw,
    (f.wind_mw + f.hydro_mw + f.solar_mw) AS renewable_generation_mw,
    f.selected_generation_mw,
    f.consumption_mw,
    f.selected_gen_balance_mw,
    f.wind_share_selected_ratio,
    f.price_eur_per_mwh,
    f.temp_c,
    f.wind_speed_ms,
    ROUND(
        CASE
            WHEN f.selected_generation_mw > 0 THEN
                (f.wind_mw * 11.0 + f.nuclear_mw * 12.0 + f.hydro_mw * 24.0 + f.solar_mw * 41.0)
                / f.selected_generation_mw
            ELSE 0.0
        END::numeric,
        2
    ) AS est_grid_carbon_g_per_kwh
FROM {fct_power_market_table_name} f
JOIN {dim_datetime_table_name} d ON f.start_time_utc = d.start_time_utc;

CREATE OR REPLACE VIEW {vw_monthly_market_summary_name} AS
SELECT
    year,
    month,
    season,
    COUNT(*) AS interval_count,
    ROUND(AVG(price_eur_per_mwh)::numeric, 2) AS avg_price_eur_mwh,
    SUM(CASE WHEN price_eur_per_mwh <= 0 THEN 1 ELSE 0 END) AS non_positive_price_intervals,
    ROUND(AVG(temp_c)::numeric, 2) AS avg_temp_c,
    ROUND(AVG(wind_speed_ms)::numeric, 2) AS avg_wind_speed_ms,
    ROUND(AVG(wind_mw)::numeric, 1) AS avg_wind_mw,
    ROUND(AVG(nuclear_mw)::numeric, 1) AS avg_nuclear_mw,
    ROUND(AVG(hydro_mw)::numeric, 1) AS avg_hydro_mw,
    ROUND(AVG(solar_mw)::numeric, 1) AS avg_solar_mw,
    ROUND(AVG(consumption_mw)::numeric, 1) AS avg_consumption_mw,
    ROUND(AVG(est_grid_carbon_g_per_kwh)::numeric, 2) AS avg_carbon_g_per_kwh
FROM {vw_power_market_360_name}
GROUP BY year, month, season
ORDER BY year, month;

CREATE OR REPLACE VIEW {vw_regional_capacity_by_fuel_name} AS
WITH unit_enriched AS (
    SELECT
        u.unit_id,
        u.unit_name,
        u.municipality,
        u.operator,
        u.psr_type,
        COALESCE(s.fuel_category, u.psr_type) AS fuel_category,
        COALESCE(s.source_category, 'Clean') AS source_category,
        u.capacity_mw,
        u.latitude,
        u.longitude,
        c.admin_name AS region_name
    FROM {dim_power_unit_table_name} u
    LEFT JOIN {dim_energy_source_table_name} s
        ON LOWER(u.psr_type) = LOWER(s.psr_type)
        OR LOWER(u.psr_type) = LOWER(s.fuel_category)
    LEFT JOIN {dim_fi_city_table_name} c
        ON LOWER(u.municipality) = LOWER(c.city_ascii)
)
SELECT
    municipality,
    COALESCE(region_name, 'Regional Grid') AS region_name,
    fuel_category,
    source_category,
    COUNT(unit_id) AS unit_count,
    ROUND(SUM(capacity_mw)::numeric, 1) AS total_capacity_mw,
    ROUND(AVG(latitude)::numeric, 4) AS latitude,
    ROUND(AVG(longitude)::numeric, 4) AS longitude,
    ROW_NUMBER() OVER (
        PARTITION BY municipality
        ORDER BY SUM(capacity_mw) DESC
    ) AS dominance_rank
FROM unit_enriched
GROUP BY municipality, COALESCE(region_name, 'Regional Grid'), fuel_category, source_category;
"""
