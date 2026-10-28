import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, StringType

try:
    BASE_DIR = Path(__file__).resolve().parent.parent
except NameError:
    BASE_DIR = Path.cwd().parent if Path.cwd().name != "energy_lakehouse" else Path.cwd()

bronze_dir = BASE_DIR / "data/bronze/entsoe/day_ahead_prices/FI"
xml_files = sorted(bronze_dir.glob("*.xml"))

if not xml_files:
    raise FileNotFoundError(f"No XML files found in {bronze_dir}")

print(f"Found {len(xml_files)} XML file(s): {[f.name for f in xml_files]}")


# Parse XML into 15-minute records (with gap filling & resolution check)
def parse_entsoe_xml(xml_path: Path):
    root = ET.parse(xml_path).getroot()
    ns = {"n": root.tag.split("}")[0].strip("{")}
    records = []

    for ts in root.findall("n:TimeSeries", ns):
        for period in ts.findall("n:Period", ns):
            start_str = period.find("n:timeInterval/n:start", ns).text
            end_str = period.find("n:timeInterval/n:end", ns).text
            res = period.find("n:resolution", ns).text  # PT15M or PT60M

            start_dt = datetime.strptime(start_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
            end_dt = datetime.strptime(end_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)

            # Map existing points: {position: price}
            points = {
                int(p.find("n:position", ns).text): float(p.find("n:price.amount", ns).text)
                for p in period.findall("n:Point", ns)
            }

            step_mins = 15 if res == "PT15M" else 60
            total_steps = int((end_dt - start_dt).total_seconds() // (step_mins * 60)) # Summer time is over so extra calculation is necesary

            last_price = None
            for pos in range(1, total_steps + 1):
                # Fill missing positions (A03 curve type) with previous price
                if pos in points:
                    last_price = points[pos]
                
                base_time = start_dt + timedelta(minutes=(pos - 1) * step_mins)

                if res == "PT15M":
                    records.append((base_time, last_price, res))
                elif res == "PT60M":
                    # Expand 1 hour into four 15-minute intervals
                    for q in range(4):
                        records.append((base_time + timedelta(minutes=15 * q), last_price, res))

    return records


all_records = []
for xml_file in xml_files:
    all_records.extend(parse_entsoe_xml(xml_file))


# Create Spark DataFrame and standardize columns
spark = (
    SparkSession.builder
    .appName("Silver_ENTSOE_Price")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.sql.session.timeZone", "UTC") # to prevent double time differences
    .getOrCreate()
)

schema = StructType([
    StructField("start_time_utc", TimestampType(), False),
    StructField("price_eur_per_mwh", DoubleType(), True),
    StructField("source_resolution", StringType(), True),
])

price_df = (
    spark.createDataFrame(all_records, schema=schema)
    .dropDuplicates(["start_time_utc"])
    .withColumn(
        "start_time_helsinki",
        F.from_utc_timestamp(F.col("start_time_utc"), "Europe/Helsinki")
    )
    .orderBy("start_time_utc")
)

# Inspect the data (Schema, Row count, Sample rows, Summary stats)
print(f"\nTotal 15-min rows: {price_df.count()}")
price_df.printSchema()

print("\n--- Sample Data (Top 10 Rows) ---")
price_df.select(
    "start_time_utc", 
    "start_time_helsinki", 
    "price_eur_per_mwh", 
    "source_resolution"
).show(10, truncate=False)

print("\n--- Price Summary Statistics (Min / Avg / Max EUR/MWh) ---")
price_df.select(
    F.min("price_eur_per_mwh").alias("min_price"),
    F.round(F.avg("price_eur_per_mwh"), 2).alias("avg_price"),
    F.max("price_eur_per_mwh").alias("max_price"),
).show()

# Save to Silver Parquet
silver_out = str(BASE_DIR / "data/silver/entsoe/day_ahead_price")
price_df.write.mode("overwrite").format("parquet").save(silver_out)
print(f"Successfully saved Silver price table to: {silver_out}")