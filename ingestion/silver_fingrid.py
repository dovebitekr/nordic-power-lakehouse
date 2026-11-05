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

# Initialize SparkSession
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
    Parse Bronze JSON, standardize timestamps, downsample intervals if needed,
    clamp negative generation values, and return Silver DataFrame.
    """
    # Read JSON and explode data array
    raw_df = (
        spark.read.option("multiline", "true")
        .json(json_path)
        .select(F.explode(F.col("data")).alias("record"))
        .select(
            F.col("record.startTime").alias("startTime"),
            F.col("record.endTime").alias("endTime"),
            F.col("record.value").cast("double").alias(value_col_name)
        )
    )
    
    # Resample high frequency data or keep standard interval data
    if resample_15min:
        standardized_df = (
            raw_df
            .withColumn("timestamp_utc", F.to_timestamp(F.col("startTime")))
            .groupBy(F.window(F.col("timestamp_utc"), "15 minutes").alias("time_window"))
            .agg(F.round(F.avg(value_col_name), 2).alias(value_col_name))
            .select(
                F.col("time_window.start").alias("start_time_utc"),
                F.col("time_window.end").alias("end_time_utc"),
                F.col(value_col_name)
            )
        )
    else:
        standardized_df = (
            raw_df
            .withColumn("start_time_utc", F.to_timestamp(F.col("startTime")))
            .withColumn("end_time_utc", F.to_timestamp(F.col("endTime")))
            .select("start_time_utc", "end_time_utc", value_col_name)
        )
        
    # Add local Helsinki timestamps and calendar features
    standardized_df = (
        standardized_df
        .withColumn("start_time_helsinki", F.from_utc_timestamp(F.col("start_time_utc"), "Europe/Helsinki"))
        .withColumn("end_time_helsinki", F.from_utc_timestamp(F.col("end_time_utc"), "Europe/Helsinki"))
        # Extra time features for partitioning and analysis
        .withColumn("date_start_helsinki", F.to_date(F.col("start_time_helsinki")))
        .withColumn("year", F.year(F.col("start_time_helsinki")))
        .withColumn("month", F.month(F.col("start_time_helsinki")))
        .withColumn("hour_start_helsinki", F.hour(F.col("start_time_helsinki")))
        .withColumn("minute_start_helsinki", F.minute(F.col("start_time_helsinki")))
        # Day of week and weekend flag
        .withColumn("day_of_week", F.dayofweek(F.col("start_time_helsinki")))
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), True).otherwise(False))
        # Peak hour flag
        .withColumn(
            "is_peak_hour",
            (~F.col("is_weekend")) & (F.col("hour_start_helsinki") >= 8) & (F.col("hour_start_helsinki") < 20)
        )
    )

    # Clamp negative generation values to zero
    if is_generation:
        neg_count = standardized_df.filter(F.col(value_col_name) < 0).count()
        if neg_count > 0:
            logging.info("Clamped %s negative generation records to zero for %s", neg_count, value_col_name)
        cleaned_df = standardized_df.withColumn(
            value_col_name,
            F.when(F.col(value_col_name) < 0, 0.0).otherwise(F.col(value_col_name))
        )
    else:
        cleaned_df = standardized_df

    # Remove duplicates and sort by timestamp
    return (
        cleaned_df
        .dropDuplicates(["start_time_utc"])
        .orderBy(F.col("start_time_utc").asc())
    )

# Task configuration for core metrics
BASE_DIR = Path(__file__).resolve().parent.parent
bronze_dir = BASE_DIR / "data/bronze/fingrid"

pipeline_tasks = [
    {
        "pattern": "wind_generation_*.json",
        "col": "wind_mw",
        "is_gen": True,
        "resample": False,
        "out": str(BASE_DIR / "data/silver/fingrid/wind_generation"),
    },
    {
        "pattern": "nuclear_generation_*.json",
        "col": "nuclear_mw",
        "is_gen": True,
        "resample": True,
        "out": str(BASE_DIR / "data/silver/fingrid/nuclear_generation"),
    },
    {
        "pattern": "hydro_generation_*.json",
        "col": "hydro_mw",
        "is_gen": True,
        "resample": True,
        "out": str(BASE_DIR / "data/silver/fingrid/hydro_generation"),
    },
    {
        "pattern": "total_consumption_*.json",
        "col": "consumption_mw",
        "is_gen": False,
        "resample": False,
        "out": str(BASE_DIR / "data/silver/fingrid/total_consumption"),
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
    df.coalesce(1).write.mode("overwrite").partitionBy("year").format("parquet").save(task["out"])

logging.info("All silver datasets successfully processed and saved")
