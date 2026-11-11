from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from common.namings import NordicDataCategory

# Explicit PySpark schemas indexed by NordicDataCategory enum
NORDIC_SCHEMAS = {
    NordicDataCategory.FINGRID_SERIES: StructType([
        StructField("startTime", StringType(), False),
        StructField("value", DoubleType(), True),
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
ENTSOE_PRICE_SCHEMA = NORDIC_SCHEMAS[NordicDataCategory.ENTSOE_PRICE]
WEATHER_SILVER_SCHEMA = NORDIC_SCHEMAS[NordicDataCategory.WEATHER_SERIES]

# Reference CSV column type schema for Finnish power units
POWER_UNIT_OVERLAY_SCHEMA = {
    "unit_id": str,
    "unit_name": str,
    "psr_type": str,
    "latitude": float,
    "longitude": float,
    "capacity_mw": float,
    "municipality": str,
    "operator": str,
}
