import argparse
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Parse command line arguments
parser = argparse.ArgumentParser(description="Build Gold power market dataset")
parser.add_argument("--start", type=str, default=None, help="Start date in YYYY-MM-DD format")
parser.add_argument("--end", type=str, default=None, help="End date in YYYY-MM-DD format")
args = parser.parse_args()

# Path configuration
BASE_DIR = Path(__file__).resolve().parent.parent

# Initialize spark session
spark = (
    SparkSession.builder
    .appName("Nordic-Power-Lakehouse-Gold")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# Silver metric paths
metrics = {
    "wind_mw": BASE_DIR / "data/silver/fingrid/wind_generation",
    "nuclear_mw": BASE_DIR / "data/silver/fingrid/nuclear_generation",
    "hydro_mw": BASE_DIR / "data/silver/fingrid/hydro_generation",
    "consumption_mw": BASE_DIR / "data/silver/fingrid/total_consumption",
    "price_eur_per_mwh": BASE_DIR / "data/silver/entsoe/day_ahead_price"
}

# Load silver tables and detect dynamic bounds when arguments are omitted
silver_dfs = {}
min_timestamps = []
max_timestamps = []

for col_name, path in metrics.items():
    sdf = spark.read.parquet(str(path)).select("start_time_utc", col_name)
    silver_dfs[col_name] = sdf
    if args.start is None or args.end is None:
        bounds = sdf.agg(
            F.date_format(F.min("start_time_utc"), "yyyy-MM-dd HH:mm:ss").alias("min_ts"),
            F.date_format(F.max("start_time_utc"), "yyyy-MM-dd HH:mm:ss").alias("max_ts")
        ).collect()[0]
        min_timestamps.append(bounds["min_ts"])
        max_timestamps.append(bounds["max_ts"])

# Calculate start and end bounds
if args.start is not None:
    start_utc = f"{args.start} 00:00:00"
else:
    start_utc = max(min_timestamps)

if args.end is not None:
    end_dt = datetime.strptime(args.end, "%Y-%m-%d") - timedelta(minutes=15)
    end_utc = end_dt.strftime("%Y-%m-%d %H:%M:%S")
else:
    end_utc = min(max_timestamps)

logging.info("Building Gold time grid from %s to %s UTC", start_utc, end_utc)

# Generate time grid
time_grid = (
    spark.sql(f"""
        SELECT explode(sequence(
            to_timestamp('{start_utc}'),
            to_timestamp('{end_utc}'),
            interval 15 minutes
        )) AS start_time_utc
    """)
    .withColumn("start_time_helsinki", F.from_utc_timestamp("start_time_utc", "Europe/Helsinki"))
    .withColumn("date_start_helsinki", F.to_date("start_time_helsinki"))
    .withColumn("minute_start_helsinki", F.minute("start_time_helsinki"))
    .withColumn("day_of_week", F.dayofweek("start_time_helsinki"))
    .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))
    .withColumn(
        "is_peak_hour",
        (~F.col("is_weekend")) & (F.hour("start_time_helsinki") >= 8) & (F.hour("start_time_helsinki") < 20)
    )
)

# Join metrics on timestamp
joined_df = time_grid
for col_name, sdf in silver_dfs.items():
    joined_df = joined_df.join(sdf, on="start_time_utc", how="left")

# Log missing values before forward fill so human operators can inspect anomalies
pre_fill_missing = {}
for col_name in metrics.keys():
    missing_rows = (
        joined_df
        .filter(F.col(col_name).isNull())
        .select(F.date_format("start_time_utc", "yyyy-MM-dd HH:mm:ss").alias("ts"))
        .orderBy("ts")
        .collect()
    )
    if missing_rows:
        missing_ts = [row["ts"] for row in missing_rows]
        pre_fill_missing[col_name] = len(missing_ts)
        logging.warning(
            "Detected %s missing intervals in %s at timestamps: %s",
            len(missing_ts),
            col_name,
            missing_ts,
        )

# Window specification restricted to single preceding interval
w = Window.orderBy("start_time_utc").rowsBetween(-1, 0)

# Forward fill missing metrics and compute derived features
gold_df = (
    joined_df
    .withColumn("wind_mw", F.last("wind_mw", ignorenulls=True).over(w))
    .withColumn("consumption_mw", F.last("consumption_mw", ignorenulls=True).over(w))
    .withColumn("selected_generation_mw", F.round(F.col("wind_mw") + F.col("nuclear_mw") + F.col("hydro_mw"), 3))
    .withColumn("net_load_ex_solar_mw", F.round(F.col("consumption_mw") - F.col("wind_mw"), 3))
    .withColumn("selected_gen_balance_mw", F.round(F.col("selected_generation_mw") - F.col("consumption_mw"), 3))
    .withColumn("wind_share_selected_ratio", F.when(F.col("selected_generation_mw") > 0, F.round((F.col("wind_mw")/F.col("selected_generation_mw")), 3)).otherwise(0.0))
    .withColumn("year", F.year("start_time_helsinki"))
    .withColumn("month", F.month("start_time_helsinki"))
    .orderBy("start_time_utc")
)

# Verify post-fill status to distinguish single-step heals from unresolved outages
for col_name, pre_count in pre_fill_missing.items():
    remaining_nulls = gold_df.filter(F.col(col_name).isNull()).count()
    if remaining_nulls == 0:
        logging.info("Auto-healed %s single-interval glitches in %s", pre_count, col_name)
    else:
        logging.error(
            "Unresolved outage: %s null intervals remain in %s after single-step fill",
            remaining_nulls,
            col_name,
        )

# Save output partitioned by year
outdir = str(BASE_DIR / "data/gold/power_market_15min")
gold_df.coalesce(1).write.mode("overwrite").partitionBy("year").format("parquet").save(outdir)

logging.info("Successfully saved Gold dataset to %s", outdir)
logging.info("Total Gold records count: %s", gold_df.count())
