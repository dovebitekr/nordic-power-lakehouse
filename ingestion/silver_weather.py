import logging
import os
from pathlib import Path
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from common.schema import WEATHER_SILVER_SCHEMA

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Initialise SparkSession
spark = (
    SparkSession.builder
    .appName("Nordic-Power-Lakehouse-Silver-Weather")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)


def parse_open_meteo_hourly(spark_session, json_paths: list[str], raw_field: str, target_col: str):
    """
    Parse Open-Meteo hourly JSON arrays and upsample to quarter-hour UTC intervals.
    """
    # Zip parallel time and value arrays from the hourly object and explode rows
    raw_df = (
        spark_session.read.option("multiline", "true")
        .json(json_paths)
        .select(
            F.explode(
                F.arrays_zip(
                    F.col("hourly.time"),
                    F.col(f"hourly.{raw_field}"),
                )
            ).alias("rec")
        )
        .select(
            F.to_timestamp(F.col("rec.time"), "yyyy-MM-dd'T'HH:mm").alias("hour_utc"),
            F.round(F.col(f"rec.{raw_field}").cast("double"), 2).alias(target_col),
        )
        .filter(F.col("hour_utc").isNotNull())
    )

    # Expand each hourly reading across four quarter-hour intervals within that hour
    quarter_hour_df = (
        raw_df
        .withColumn(
            "start_time_utc",
            F.explode(
                F.sequence(
                    F.col("hour_utc"),
                    F.col("hour_utc") + F.expr("INTERVAL 45 MINUTES"),
                    F.expr("INTERVAL 15 MINUTES"),
                )
            ),
        )
        .select("start_time_utc", target_col)
        .dropDuplicates(["start_time_utc"])
    )
    return quarter_hour_df


bronze_dir = BASE_DIR / "data/bronze/weather"
temp_files = sorted(bronze_dir.glob("helsinki_temp_*.json"))
wind_files = sorted(bronze_dir.glob("vaasa_wind_*.json"))

if not temp_files or not wind_files:
    logging.error("Missing bronze weather JSON files in %s", bronze_dir)
    sys.exit(1)

logging.info(
    "Processing weather files: %s temperature files, %s wind files",
    len(temp_files),
    len(wind_files),
)

temp_df = parse_open_meteo_hourly(
    spark_session=spark,
    json_paths=[str(p) for p in temp_files],
    raw_field="temperature_2m",
    target_col="temp_c",
)

wind_df = parse_open_meteo_hourly(
    spark_session=spark,
    json_paths=[str(p) for p in wind_files],
    raw_field="wind_speed_10m",
    target_col="wind_speed_ms",
)

# Join temperature and wind series and validate against Silver weather schema
core_cols = [field.name for field in WEATHER_SILVER_SCHEMA.fields]
weather_silver_df = (
    temp_df.join(wind_df, on="start_time_utc", how="inner")
    .select(*core_cols)
    .withColumn("start_time_helsinki", F.from_utc_timestamp(F.col("start_time_utc"), "Europe/Helsinki"))
    .withColumn("year", F.year(F.col("start_time_helsinki")))
    .orderBy(F.col("start_time_utc").asc())
)

row_count = weather_silver_df.count()
if row_count == 0:
    logging.error("Silver weather dataset has zero records")
    sys.exit(1)

out_path = str(BASE_DIR / "data/silver/weather/finnish_weather")
weather_silver_df.coalesce(1).write.mode("overwrite").partitionBy("year").format("parquet").save(out_path)
logging.info("Saved %s Silver weather records to %s", row_count, out_path)
