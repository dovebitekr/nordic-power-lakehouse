import logging
import os
from pathlib import Path
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from common.file_config import BRONZE_DIR, SILVER_DIR, config
from common.schema import ENTSOE_PRICE_SCHEMA

bronze_dir = BRONZE_DIR / "entsoe/day_ahead_prices/FI"
xml_files = sorted(bronze_dir.glob("*.xml"))

if not xml_files:
    logging.error("No XML files found in %s", bronze_dir)
    sys.exit(1)

logging.info("Found %s XML files in bronze directory", len(xml_files))

# Parse XML into interval records with resolution check
def parse_entsoe_xml(xml_path: Path):
    root = ET.parse(xml_path).getroot()
    ns = {"n": root.tag.split("}")[0].strip("{")}
    records = []
    filled_positions = 0

    for ts in root.findall("n:TimeSeries", ns):
        for period in ts.findall("n:Period", ns):
            start_str = period.find("n:timeInterval/n:start", ns).text
            end_str = period.find("n:timeInterval/n:end", ns).text
            res = period.find("n:resolution", ns).text

            start_dt = datetime.strptime(start_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
            end_dt = datetime.strptime(end_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)

            # Map existing points
            points = {
                int(p.find("n:position", ns).text): float(p.find("n:price.amount", ns).text)
                for p in period.findall("n:Point", ns)
            }

            step_mins = 15 if res == "PT15M" else 60
            total_steps = int((end_dt - start_dt).total_seconds() // (step_mins * 60))

            last_price = None
            for pos in range(1, total_steps + 1):
                # Fill missing curve positions with previous price
                if pos in points:
                    last_price = points[pos]
                else:
                    filled_positions += 1
                
                base_time = start_dt + timedelta(minutes=(pos - 1) * step_mins)

                if res == "PT15M":
                    records.append((base_time, last_price, res))
                elif res == "PT60M":
                    # Expand hour into multiple quarter hour intervals
                    for q in range(4):
                        records.append((base_time + timedelta(minutes=15 * q), last_price, res))

    return records, filled_positions

all_records = []
total_curve_fills = 0
for xml_file in xml_files:
    file_records, file_fills = parse_entsoe_xml(xml_file)
    all_records.extend(file_records)
    total_curve_fills += file_fills

logging.info("Parsed %s raw intervals (decompressed %s omitted curve positions)", len(all_records), total_curve_fills)

# Create Spark DataFrame and standardise columns
spark = (
    SparkSession.builder
    .appName("Silver_ENTSOE_Price")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

price_df = (
    spark.createDataFrame(all_records, schema=ENTSOE_PRICE_SCHEMA)
    .dropDuplicates(["start_time_utc"])
    .withColumn(
        "start_time_helsinki",
        F.from_utc_timestamp(F.col("start_time_utc"), "Europe/Helsinki")
    )
    .withColumn("year", F.year(F.col("start_time_helsinki")))
    .orderBy("start_time_utc")
)

# Inspect the data schema and summary
row_count = price_df.count()
if row_count == 0:
    logging.error("Silver price table has zero records")
    sys.exit(1)

logging.info("Total price rows: %s", row_count)
price_df.printSchema()

# Save to Silver Parquet
silver_out = str(SILVER_DIR / "entsoe" / config["entsoe"]["table_name"])
partition_cols = config["entsoe"]["partition_cols"]
price_df.coalesce(1).write.mode("overwrite").partitionBy(*partition_cols).format("parquet").save(silver_out)
logging.info("Successfully saved Silver price table to %s", silver_out)
