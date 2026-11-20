import logging
import os
from pathlib import Path
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Initialise SparkSession
spark = (
    SparkSession.builder
    .appName("Nordic-Power-Lakehouse-Silver")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

def process_fingrid_bronze_to_silver(
    spark, 
    json_path: str, 
    value_col_name: str, 
    is_generation: bool = True,
    resample_15min: bool = False
):
    """
    Parse Bronze JSON, standardise timestamps, downsample intervals if needed,
    clamp negative generation values, and return Silver DataFrame.
    """
    # Read JSON and explode data array
    raw_df = (
        spark.read.option("multiline", "true")
        .json(json_path)
        .select(F.explode(F.col("data")).alias("record"))
        .select(
            F.col("record.startTime").alias("startTime"),
            F.col("record.value").cast("double").alias(value_col_name)
        )
    )
    
    # Resample high frequency data or keep standard interval data
    if resample_15min:
        standardised_df = (
            raw_df
            .withColumn("timestamp_utc", F.to_timestamp(F.col("startTime")))
            .groupBy(F.window(F.col("timestamp_utc"), "15 minutes").alias("time_window"))
            .agg(F.round(F.avg(value_col_name), 2).alias(value_col_name))
            .select(
                F.col("time_window.start").alias("start_time_utc"),
                F.col(value_col_name)
            )
        )
    else:
        standardised_df = (
            raw_df
            .withColumn("start_time_utc", F.to_timestamp(F.col("startTime")))
            .select("start_time_utc", value_col_name)
        )

    # Add local Helsinki timestamp and partition year
    standardised_df = (
        standardised_df
        .withColumn("start_time_helsinki", F.from_utc_timestamp(F.col("start_time_utc"), "Europe/Helsinki"))
        .withColumn("year", F.year(F.col("start_time_helsinki")))
    )

    # Clamp negative generation values to zero
    if is_generation:
        neg_count = standardised_df.filter(F.col(value_col_name) < 0).count()
        if neg_count > 0:
            logging.info("Clamped %s negative generation records to zero for %s", neg_count, value_col_name)
        cleaned_df = standardised_df.withColumn(
            value_col_name,
            F.when(F.col(value_col_name) < 0, 0.0).otherwise(F.col(value_col_name))
        )
    else:
        cleaned_df = standardised_df

    # Remove duplicates and sort by timestamp
    return (
        cleaned_df
        .dropDuplicates(["start_time_utc"])
        .orderBy(F.col("start_time_utc").asc())
    )

# Task configuration for core metrics
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from common.file_config import BRONZE_DIR, SILVER_DIR, config

bronze_dir = BRONZE_DIR / "fingrid"
silver_fingrid_dir = SILVER_DIR / "fingrid"
partition_cols = config["fingrid"]["partition_cols"]

pipeline_tasks = [
    {
        "pattern": "wind_generation_*.json",
        "col": "wind_mw",
        "is_gen": True,
        "resample": False,
        "out": str(silver_fingrid_dir / "wind_generation"),
    },
    {
        "pattern": "nuclear_generation_*.json",
        "col": "nuclear_mw",
        "is_gen": True,
        "resample": True,
        "out": str(silver_fingrid_dir / "nuclear_generation"),
    },
    {
        "pattern": "hydro_generation_*.json",
        "col": "hydro_mw",
        "is_gen": True,
        "resample": True,
        "out": str(silver_fingrid_dir / "hydro_generation"),
    },
    {
        "pattern": "solar_generation_*.json",
        "col": "solar_mw",
        "is_gen": True,
        "resample": False,
        "out": str(silver_fingrid_dir / "solar_generation"),
    },
    {
        "pattern": "total_consumption_*.json",
        "col": "consumption_mw",
        "is_gen": False,
        "resample": False,
        "out": str(silver_fingrid_dir / "total_consumption"),
    },
]

# Run pipeline
for task in pipeline_tasks:
    matched_files = sorted(bronze_dir.glob(task["pattern"]))
    if not matched_files:
        logging.error("No bronze files found for %s matching %s", task["col"], task["pattern"])
        sys.exit(1)

    logging.info("Processing metric: %s (%s bronze files)", task["col"], len(matched_files))
    
    df = process_fingrid_bronze_to_silver(
        spark=spark,
        json_path=[str(p) for p in matched_files],
        value_col_name=task["col"],
        is_generation=task["is_gen"],
        resample_15min=task["resample"]
    )
    
    row_count = df.count()
    if row_count == 0:
        logging.error("Transformed dataset for %s has zero records", task["col"])
        sys.exit(1)

    logging.info("Transformed %s with %s records", task["col"], row_count)
    
    # Save to Parquet
    df.coalesce(1).write.mode("overwrite").partitionBy(*partition_cols).format("parquet").save(task["out"])

logging.info("All silver datasets successfully processed and saved")
