from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pathlib import Path

# Initialize SparkSession
spark = (
    SparkSession.builder
    .appName("Nordic-Power-Lakehouse-Silver")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
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
    Parse Bronze JSON, standardize timestamps, downsample 3-min to 15-min if needed,
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
    
    # Resample 3-min data (Nuclear, Hydro) or keep 15-min data (Wind, Consumption)
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
        # Extra time features for partitioning & analysis
        .withColumn("date_start_helsinki", F.to_date(F.col("start_time_helsinki")))
        .withColumn("hour_start_helsinki", F.hour(F.col("start_time_helsinki")))
        .withColumn("minute_start_helsinki", F.minute(F.col("start_time_helsinki")))
        # Day of week and weekend flag (1: Sun, 7: Sat)
        .withColumn("day_of_week", F.dayofweek(F.col("start_time_helsinki")))
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), True).otherwise(False))
        # Peak-hour flag (07:00 - 20:00)
        .withColumn(
            "is_peak_hour",
            F.when((F.col("hour_start_helsinki") >= 7) & (F.col("hour_start_helsinki") < 20), True).otherwise(False)
        )
    )

    # Clamp negative generation values to zero
    if is_generation:
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

# Task configuration for 4 core metrics
BASE_DIR = Path(__file__).resolve().parent.parent

pipeline_tasks = [
    {
        "path": str(BASE_DIR / "data/bronze/fingrid/wind_generation_sample.json"),
        "col": "wind_mw",
        "is_gen": True,
        "resample": False,
        "out": str(BASE_DIR / "data/silver/fingrid/wind_generation")
    },
    {
        "path": str(BASE_DIR / "data/bronze/fingrid/nuclear_generation_sample.json"),
        "col": "nuclear_mw",
        "is_gen": True,
        "resample": True,
        "out": str(BASE_DIR / "data/silver/fingrid/nuclear_generation")
    },
    {
        "path": str(BASE_DIR / "data/bronze/fingrid/hydro_generation_sample.json"),
        "col": "hydro_mw",
        "is_gen": True,
        "resample": True,
        "out": str(BASE_DIR / "data/silver/fingrid/hydro_generation")
    },
    {
        "path": str(BASE_DIR / "data/bronze/fingrid/total_consumption_sample.json"),
        "col": "consumption_mw",
        "is_gen": False,
        "resample": False,
        "out": str(BASE_DIR / "data/silver/fingrid/total_consumption")
    }
]

# Run pipeline
for task in pipeline_tasks:
    print(f"Processing: {task['col']}...")
    
    df = process_fingrid_bronze_to_silver(
        spark=spark,
        json_path=task["path"],
        value_col_name=task["col"],
        is_generation=task["is_gen"],
        resample_15min=task["resample"]
    )
    
    row_count = df.count()
    print(f" -> {task['col']} count: {row_count} / 288")
    
    # Save to Parquet
    df.write.mode("overwrite").format("parquet").save(task["out"])

print("All Silver datasets successfully processed and saved.")

