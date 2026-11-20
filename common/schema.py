from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from common.namings import NordicDataCategory

# Explicit PySpark schemas indexed by NordicDataCategory enum (prevents inferSchema drift)
NORDIC_SCHEMAS = {
    NordicDataCategory.ENTSOE_UNIT_GENERATION: StructType([
        StructField("start_time_utc", TimestampType(), False),
        StructField("unit_mrid", StringType(), False),
        StructField("unit_name", StringType(), True),
        StructField("entsoe_psr_code", StringType(), False),
        StructField("generation_mw", DoubleType(), True),
        StructField("source_resolution", StringType(), True),
    ]),
    NordicDataCategory.ENTSOE_FUEL_GENERATION: StructType([
        StructField("start_time_utc", TimestampType(), False),
        StructField("entsoe_psr_code", StringType(), False),
        StructField("generation_mw", DoubleType(), True),
        StructField("source_resolution", StringType(), True),
    ]),
    NordicDataCategory.ENTSOE_PRICE: StructType([
        StructField("start_time_utc", TimestampType(), False),
        StructField("price_eur_per_mwh", DoubleType(), True),
        StructField("source_resolution", StringType(), True),
    ]),
    NordicDataCategory.WEATHER_SERIES: StructType([
        StructField("start_time_utc", TimestampType(), False),
        StructField("temp_c", DoubleType(), True),
        StructField("wind_speed_ms", DoubleType(), True),
    ]),
}

# Direct aliases for Silver layer imports
ENTSOE_UNIT_GEN_SCHEMA = NORDIC_SCHEMAS[NordicDataCategory.ENTSOE_UNIT_GENERATION]
ENTSOE_FUEL_GEN_SCHEMA = NORDIC_SCHEMAS[NordicDataCategory.ENTSOE_FUEL_GENERATION]
ENTSOE_PRICE_SCHEMA = NORDIC_SCHEMAS[NordicDataCategory.ENTSOE_PRICE]
WEATHER_SILVER_SCHEMA = NORDIC_SCHEMAS[NordicDataCategory.WEATHER_SERIES]

# Reference CSV column type schema for Finnish power unit master table
POWER_UNIT_MASTER_SCHEMA = {
    "unit_mrid": str,
    "unit_name": str,
    "entsoe_psr_code": str,
    "installed_capacity_mw": float,
    "city_ascii": str,
    "region_fi": str,
    "operator": str,
    "latitude": float,
    "longitude": float,
    "is_active": bool,
}
