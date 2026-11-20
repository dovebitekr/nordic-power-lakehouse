"""
Fetch Finland ENTSO-E generation XML data (A73 per unit & A75 per fuel type)
into local Bronze storage and upload to Google Cloud Storage (GCS).
"""
import argparse
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
import sys
import time
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from google.cloud import storage
import requests

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from common.file_config import (
    BRONZE_DIR,
    GCP_PROJECT_ID,
    GCS_BUCKET_NAME,
    INITIALISE_END_DATE,
    INITIALISE_START_DATE,
    config,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

load_dotenv(BASE_DIR / ".env")
TOKEN = os.getenv("ENTSOE_API_KEY")
if not TOKEN:
    raise ValueError("ENTSOE_API_KEY is missing in .env file.")

ENTSOE_CFG = config["entsoe"]
BASE_URL = ENTSOE_CFG["base_url"]
ZONE = ENTSOE_CFG["zone"]


def upload_to_gcs(bucket, local_path: Path, gcs_blob_path: str) -> None:
    """Upload a local Bronze XML file to the GCS Data Lake bucket."""
    blob = bucket.blob(gcs_blob_path)
    blob.upload_from_filename(str(local_path))
    logging.info("Uploaded to gs://%s/%s", bucket.name, gcs_blob_path)


def fetch_dataset(
    dataset_key: str,
    start_date: str,
    end_date: str,
    chunk_days: int | None,
    overwrite: bool,
    gcs_bucket,
) -> int:
    """Fetch one ENTSO-E dataset (unit_generation or fuel_generation) in date chunks."""
    ds_cfg = ENTSOE_CFG["datasets"][dataset_key]
    doc_type = ds_cfg["document_type"]
    process_type = ds_cfg["process_type"]
    template = ds_cfg["output_file_template"]
    effective_chunk_days = chunk_days if chunk_days is not None else ds_cfg.get("max_chunk_days", 1)

    out_dir = BRONZE_DIR / "entsoe" / dataset_key
    out_dir.mkdir(parents=True, exist_ok=True)

    current_start = datetime.strptime(start_date, "%Y-%m-%d")
    global_end = datetime.strptime(end_date, "%Y-%m-%d")
    failed_chunks = 0

    while current_start < global_end:
        current_end = min(current_start + timedelta(days=effective_chunk_days), global_end)
        p_start = current_start.strftime("%Y%m%d0000")
        p_end = current_end.strftime("%Y%m%d0000")
        file_name = template.format(p_start, p_end)
        out_path = out_dir / file_name
        gcs_path = f"bronze/entsoe/{dataset_key}/{file_name}"

        if out_path.exists() and out_path.stat().st_size > 0 and not overwrite:
            logging.info("Skipping existing local file: %s", file_name)
            if gcs_bucket is not None:
                upload_to_gcs(gcs_bucket, out_path, gcs_path)
            current_start = current_end
            continue

        params = {
            "securityToken": TOKEN,
            "documentType": doc_type,
            "processType": process_type,
            "in_Domain": ZONE,
            "periodStart": p_start,
            "periodEnd": p_end,
        }
        logging.info("Fetching %s (%s) from %s to %s", dataset_key, doc_type, p_start, p_end)

        response = None
        for attempt in range(1, 4):
            try:
                response = requests.get(BASE_URL, params=params, timeout=45)
                if response.status_code == 200:
                    break
                logging.warning("HTTP %s on attempt %s; retrying...", response.status_code, attempt)
                time.sleep(attempt * 2)
            except requests.RequestException as exc:
                logging.warning("Network error on attempt %s: %s", attempt, exc)
                time.sleep(attempt * 2)

        if response is not None and response.status_code == 200:
            root = ET.fromstring(response.content)
            ns = {"n": root.tag.split("}")[0].strip("{")}
            ts_count = len(root.findall("n:TimeSeries", ns))
            if ts_count == 0:
                logging.error("Empty TimeSeries for %s (%s -> %s)", dataset_key, p_start, p_end)
                failed_chunks += 1
            else:
                out_path.write_bytes(response.content)
                logging.info("Saved %s (%s time series)", file_name, ts_count)
                if gcs_bucket is not None:
                    upload_to_gcs(gcs_bucket, out_path, gcs_path)
        else:
            status = response.status_code if response is not None else "No response"
            logging.error("Failed %s (%s -> %s): %s", dataset_key, p_start, p_end, status)
            failed_chunks += 1

        time.sleep(1.5)
        current_start = current_end

    return failed_chunks


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Finland ENTSO-E A73 & A75 generation data")
    parser.add_argument(
        "--dataset",
        choices=["unit_generation", "fuel_generation", "all"],
        default="all",
        help="Which ENTSO-E generation dataset to fetch",
    )
    parser.add_argument("--start", type=str, default=INITIALISE_START_DATE, help="Start YYYY-MM-DD")
    parser.add_argument("--end", type=str, default=INITIALISE_END_DATE, help="End YYYY-MM-DD")
    parser.add_argument("--chunk-days", type=int, default=None, help="Override config max_chunk_days")
    parser.add_argument("--overwrite", action="store_true", help="Re-download existing files")
    parser.add_argument("--skip-gcs", action="store_true", help="Save locally only without GCS upload")
    args = parser.parse_args()

    gcs_bucket = None
    if not args.skip_gcs:
        client = storage.Client(project=GCP_PROJECT_ID)
        gcs_bucket = client.bucket(GCS_BUCKET_NAME)

    targets = ["unit_generation", "fuel_generation"] if args.dataset == "all" else [args.dataset]
    total_failures = 0
    for target in targets:
        total_failures += fetch_dataset(
            dataset_key=target,
            start_date=args.start,
            end_date=args.end,
            chunk_days=args.chunk_days,
            overwrite=args.overwrite,
            gcs_bucket=gcs_bucket,
        )

    if total_failures > 0:
        logging.error("Finished with %s failed chunks", total_failures)
        sys.exit(1)

    logging.info("ENTSO-E generation Bronze ingestion and GCS upload completed!")


if __name__ == "__main__":
    main()
