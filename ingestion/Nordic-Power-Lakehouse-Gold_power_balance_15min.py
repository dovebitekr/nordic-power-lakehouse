from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
spark = (
    SparkSession.builder
    .appName("Nordic-Power-Lakehouse-Gold_power_balance_15min")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# Task metadata for reading Silver Parquet
silver_tasks = [
    {
        "energy_name": "wind",
        "path": BASE_DIR / "data/silver/fingrid/wind_generation",
        "value_col": "wind_mw",
        "is_base": True  # Keep all calendar/time metadata columns
    },
    {
        "energy_name": "nuclear",
        "path": BASE_DIR / "data/silver/fingrid/nuclear_generation",
        "value_col": "nuclear_mw",
        "is_base": False # Keep only join key and value
    },
    {
        "energy_name": "hydro",
        "path": BASE_DIR / "data/silver/fingrid/hydro_generation",
        "value_col": "hydro_mw",
        "is_base": False
    },
    {
        "energy_name": "total_consumption",
        "path": BASE_DIR / "data/silver/fingrid/total_consumption",
        "value_col": "consumption_mw",
        "is_base": False
    },
    {
        "energy_name" : "price",
        "path": BASE_DIR / "data/silver/entsoe/day_ahead_price",
        "value_col": "price_eur_per_mwh",
        "is_base": False
    }
]

def load_silver_dataset(path, value_col: str, is_base: bool = False):
    """
    Read Silver Parquet. If base dataset, keep all time metadata;
    otherwise, select only join key and value column to avoid collisions.
    """
    df = spark.read.parquet(str(path))
    
    if is_base:
        # Keep time metadata and the metric value
        return df.select(
            F.col("start_time_utc"),
            F.col("end_time_utc"),
            F.col("start_time_helsinki"),
            F.col("date_start_helsinki"),
            F.col("hour_start_helsinki"),
            F.col("minute_start_helsinki"),
            F.col("day_of_week"),
            F.col("is_weekend"),
            F.col("is_peak_hour"),
            F.col(value_col)
        )
    else:
        # Keep only join key and metric value
        return df.select(
            F.col("start_time_utc"),
            F.col(value_col)
        )

# Load all Silver DataFrames into a dictionary
dfs = {
    t["energy_name"]: load_silver_dataset(t["path"], t["value_col"], t["is_base"])
    for t in silver_tasks
}


joined_df = (
    dfs["wind"]
    .join(dfs["nuclear"], on="start_time_utc", how="left")
    .join(dfs["hydro"], on="start_time_utc", how="left")
    .join(dfs["total_consumption"], on="start_time_utc", how="left")
    .join(dfs["price"], on="start_time_utc", how="left")
    .withColumn("selected_generation_mw", F.round(F.col("wind_mw") + F.col("nuclear_mw") + F.col("hydro_mw"), 3))
    .withColumn("net_load_ex_solar_mw", F.round(F.col("consumption_mw") - F.col("wind_mw"), 3))
    .withColumn("selected_gen_balance_mw", F.round(F.col("selected_generation_mw") - F.col("consumption_mw"), 3))
    .withColumn("wind_share_selected_ratio", F.when(F.col("selected_generation_mw") > 0, F.round((F.col("wind_mw")/F.col("selected_generation_mw")), 3)).otherwise(0.0))
    # Order by start_time_utc since it is a time series dataset
    .orderBy(F.col("start_time_utc").asc())
)

outdir = str(BASE_DIR / "data/gold/power_market_15min")
joined_df.write.mode("overwrite").format("parquet").save(outdir)





