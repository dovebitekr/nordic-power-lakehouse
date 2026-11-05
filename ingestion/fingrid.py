import os
import sys
import requests
from dotenv import load_dotenv
import json
import time
from pathlib import Path
import argparse
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")
API_KEY = os.getenv("FINGRID_API_KEY")
if not API_KEY:
    raise ValueError("FINGRID_API_KEY is not configured in the .env file")

DATASETS = {
    "wind_generation": 75,
    "nuclear_generation": 188,
    "hydro_generation": 191,
    "total_consumption": 124
}
HEADERS = {
    "x-api-key": API_KEY,
    "Accept": "application/json"
}

parser = argparse.ArgumentParser(description="Fetch Fingrid datasets")
parser.add_argument(
    "--start",
    type=str,
    default="2025-11-01",
    help="Start date in YYYY-MM-DD format",
)
parser.add_argument(
    "--end",
    type=str,
    default="2026-11-01",
    help="End date in YYYY-MM-DD format",
)
args = parser.parse_args()

OUTPUT_DIR = BASE_DIR / "data/bronze/fingrid"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Parse start and end dates
current_start = datetime.strptime(args.start, "%Y-%m-%d")
global_end = datetime.strptime(args.end, "%Y-%m-%d")
failed_requests = 0

# Loop month by month
while current_start < global_end:
    current_end = min(current_start + relativedelta(months=1), global_end)
    start_str = current_start.strftime("%Y-%m-%d")
    end_str = current_end.strftime("%Y-%m-%d")
    logging.info("Processing period: %s to %s", start_str, end_str)

    params = {
        "startTime": f"{start_str}T00:00:00.000Z",
        "endTime": f"{end_str}T00:00:00.000Z",
        "format": "json",
        "pageSize": 20000,
    }

    for metric_name, dataset_id in DATASETS.items():
        logging.info("Fetching metric: %s with id: %s", metric_name, dataset_id)
        endpoint_url = f"https://data.fingrid.fi/api/datasets/{dataset_id}/data"
        response = None
        for attempt in range(1, 4):
            try:
                response = requests.get(endpoint_url, headers=HEADERS, params=params, timeout=30)
                if response.status_code == 200:
                    break
                logging.warning("Retry attempt: HTTP %s, waiting %s seconds", response.status_code, attempt * 2)
                time.sleep(attempt * 2)
            except requests.RequestException as e:
                logging.warning("Network error on attempt: %s, waiting %s seconds", e, attempt * 2)
                time.sleep(attempt * 2)

        if response is not None and response.status_code == 200:
            payload = response.json()
            record_count = len(payload.get("data", []))
            total_count = payload.get("pagination", {}).get("total", 0)
            if record_count == 0:
                logging.error("Received empty data array for %s from %s to %s", metric_name, start_str, end_str)
                failed_requests += 1
            else:
                logging.info("Received %s of %s records for %s", record_count, total_count, metric_name)
                file_path = OUTPUT_DIR / f"{metric_name}_{start_str}_{end_str}.json"
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2)
                logging.info("Saved payload to %s", file_path.name)
        else:
            status = response.status_code if response is not None else "No response"
            text = response.text if response is not None else "Connection failed"
            logging.error("Failed request for %s status: %s response: %s", metric_name, status, text)
            failed_requests += 1

        # Throttle requests to respect API rate limits
        time.sleep(2.5)

    # Move to next month
    current_start = current_end

if failed_requests > 0:
    logging.error("Ingestion finished with %s failed requests", failed_requests)
    sys.exit(1)

logging.info("Completed ingestion pipeline successfully")
