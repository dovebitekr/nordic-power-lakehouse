"""
Transform Bronze ENTSO-E A73 (per-unit) and A75 (per-fuel-type) XML files
into schema-enforced Silver Parquet tables and upload to GCS.
"""
from datetime import datetime, timedelta, timezone
import logging
import os
from pathlib import Path
import sys
import xml.etree.ElementTree as ET
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from common.file_config import (
    BRONZE_DIR,
    GCP_PROJECT_ID,
    GCS_BUCKET_NAME,
    SILVER_DIR,
    config,
)
from common.schema import ENTSOE_FUEL_GEN_SCHEMA, ENTSOE_UNIT_GEN_SCHEMA

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def parse_a73_xml(xml_path: Path) -> list[tuple]:
    """Parse ENTSO-E A73 XML into (start_time_utc, unit_mrid, unit_name, psr_code, mw, res)."""
    root = ET.parse(xml_path).getroot()
    ns = {"n": root.tag.split("}")[0].strip("{")}
    records = []

    for ts in root.findall("n:TimeSeries", ns):
        psr_code = ts.findtext("n:MktPSRType/n:psrType", "", ns)
        unit_mrid = ts.findtext("n:MktPSRType/n:PowerSystemResources/n:mRID", "", ns)
        unit_name = ts.findtext("n:MktPSRType/n:PowerSystemResources/n:name", "", ns)
        if not unit_mrid or not psr_code:
            continue

        for period in ts.findall("n:Period", ns):
            start_str = period.findtext("n:timeInterval/n:start", "", ns)
            end_str = period.findtext("n:timeInterval/n:end", "", ns)
            res = period.findtext("n:resolution", "PT15M", ns)
            start_dt = datetime.strptime(start_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
            end_dt = datetime.strptime(end_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)

            step_mins = 15 if res == "PT15M" else 60
            total_steps = int((end_dt - start_dt).total_seconds() // (step_mins * 60))
            points = {
                int(p.findtext("n:position", "0", ns)): float(p.findtext("n:quantity", "0", ns))
                for p in period.findall("n:Point", ns)
            }

            last_val = 0.0
            for pos in range(1, total_steps + 1):
                if pos in points:
                    last_val = points[pos]
                mw = max(0.0, last_val)
                base_time = start_dt + timedelta(minutes=(pos - 1) * step_mins)
                if res == "PT15M":
                    records.append((base_time, unit_mrid, unit_name, psr_code, mw, res))
                elif res == "PT60M":
                    for q in range(4):
                        records.append(
                            (base_time + timedelta(minutes=15 * q), unit_mrid, unit_name, psr_code, mw, res)
                        )
    return records


def parse_a75_xml(xml_path: Path) -> list[tuple]:
    """Parse ENTSO-E A75 XML into (start_time_utc, psr_code, mw, res) for generation series."""
    root = ET.parse(xml_path).getroot()
    ns = {"n": root.tag.split("}")[0].strip("{")}
    records = []

    for ts in root.findall("n:TimeSeries", ns):
        # Keep only generation series (inBiddingZone_Domain present), skip self-consumption
        if ts.find("n:inBiddingZone_Domain.mRID", ns) is None:
            continue
        psr_code = ts.findtext("n:MktPSRType/n:psrType", "", ns)
        if not psr_code:
            continue

        for period in ts.findall("n:Period", ns):
            start_str = period.findtext("n:timeInterval/n:start", "", ns)
            end_str = period.findtext("n:timeInterval/n:end", "", ns)
            res = period.findtext("n:resolution", "PT15M", ns)
            start_dt = datetime.strptime(start_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
            end_dt = datetime.strptime(end_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)

            step_mins = 15 if res == "PT15M" else 60
            total_steps = int((end_dt - start_dt).total_seconds() // (step_mins * 60))
            points = {
                int(p.findtext("n:position", "0", ns)): float(p.findtext("n:quantity", "0", ns))
                for p in period.findall("n:Point", ns)
            }

            last_val = 0.0
            for pos in range(1, total_steps + 1):
                if pos in points:
                    last_val = points[pos]
                mw = max(0.0, last_val)
                base_time = start_dt + timedelta(minutes=(pos - 1) * step_mins)
                if res == "PT15M":
                    records.append((base_time, psr_code, mw, res))
                elif res == "PT60M":
                    for q in range(4):
                        records.append((base_time + timedelta(minutes=15 * q), psr_code, mw, res))
    return records


def upload_directory_to_gcs(bucket, local_dir: Path, gcs_prefix: str) -> None:
    """Upload all Parquet part files under local_dir to GCS."""
    uploaded = 0
    for parquet_file in sorted(local_dir.rglob("*.parquet")):
        rel_path = parquet_file.relative_to(local_dir).as_posix()
        blob_path = f"{gcs_prefix}/{rel_path}"
        bucket.blob(blob_path).upload_from_filename(str(parquet_file))
        uploaded += 1
    logging.info("Uploaded %s Parquet files to gs://%s/%s", uploaded, bucket.name, gcs_prefix)


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("Silver_ENTSOE_Generation")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    gcs_client = storage.Client(project=GCP_PROJECT_ID)
    gcs_bucket = gcs_client.bucket(GCS_BUCKET_NAME)

    # 1. Process A73 Unit-Level Generation
    a73_bronze = BRONZE_DIR / "entsoe/unit_generation"
    a73_files = sorted(a73_bronze.glob("*.xml"))
    if a73_files:
        a73_records = []
        for f in a73_files:
            a73_records.extend(parse_a73_xml(f))
        unit_df = (
            spark.createDataFrame(a73_records, schema=ENTSOE_UNIT_GEN_SCHEMA)
            .dropDuplicates(["start_time_utc", "unit_mrid"])
            .withColumn("start_time_helsinki", F.from_utc_timestamp("start_time_utc", "Europe/Helsinki"))
            .withColumn("year", F.year("start_time_helsinki"))
            .orderBy("start_time_utc", "unit_mrid")
        )
        a73_out = SILVER_DIR / "entsoe" / config["entsoe"]["datasets"]["unit_generation"]["table_name"]
        part_cols = config["entsoe"]["datasets"]["unit_generation"]["partition_cols"]
        unit_df.coalesce(1).write.mode("overwrite").partitionBy(*part_cols).parquet(str(a73_out))
        logging.info("Saved %s Silver unit_generation rows to %s", unit_df.count(), a73_out)
        upload_directory_to_gcs(gcs_bucket, a73_out, "silver/entsoe/unit_generation")
    else:
        logging.warning("No A73 XML files found in %s", a73_bronze)

    # 2. Process A75 Fuel-Type Generation
    a75_bronze = BRONZE_DIR / "entsoe/fuel_generation"
    a75_files = sorted(a75_bronze.glob("*.xml"))
    if a75_files:
        a75_records = []
        for f in a75_files:
            a75_records.extend(parse_a75_xml(f))
        fuel_df = (
            spark.createDataFrame(a75_records, schema=ENTSOE_FUEL_GEN_SCHEMA)
            .dropDuplicates(["start_time_utc", "entsoe_psr_code"])
            .withColumn("start_time_helsinki", F.from_utc_timestamp("start_time_utc", "Europe/Helsinki"))
            .withColumn("year", F.year("start_time_helsinki"))
            .orderBy("start_time_utc", "entsoe_psr_code")
        )
        a75_out = SILVER_DIR / "entsoe" / config["entsoe"]["datasets"]["fuel_generation"]["table_name"]
        part_cols = config["entsoe"]["datasets"]["fuel_generation"]["partition_cols"]
        fuel_df.coalesce(1).write.mode("overwrite").partitionBy(*part_cols).parquet(str(a75_out))
        logging.info("Saved %s Silver fuel_generation rows to %s", fuel_df.count(), a75_out)
        upload_directory_to_gcs(gcs_bucket, a75_out, "silver/entsoe/fuel_generation")
    else:
        logging.warning("No A75 XML files found in %s", a75_bronze)

    spark.stop()


if __name__ == "__main__":
    main()
