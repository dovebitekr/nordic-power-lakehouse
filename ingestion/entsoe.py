import os
import sys
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
import argparse
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

TOKEN = os.getenv("ENTSOE_API_KEY")
if not TOKEN:
    raise ValueError("ENTSOE_API_KEY is missing in .env file.")

parser = argparse.ArgumentParser(description="Fetch ENTSO-E Day-Ahead Prices for Finland")
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

out_dir = BASE_DIR / "data/bronze/entsoe/day_ahead_prices/FI"
out_dir.mkdir(parents=True, exist_ok=True)
ZONE = "10YFI-1--------U"
url = "https://web-api.tp.entsoe.eu/api"

# Parse start and end dates
current_start = datetime.strptime(args.start, "%Y-%m-%d")
global_end = datetime.strptime(args.end, "%Y-%m-%d")
failed_periods = 0

# Loop month by month
while current_start < global_end:
    current_end = min(current_start + relativedelta(months=1), global_end)
    start_str = current_start.strftime("%Y-%m-%d")
    end_str = current_end.strftime("%Y-%m-%d")
    p_start = current_start.strftime("%Y%m%d0000")
    p_end = current_end.strftime("%Y%m%d0000")
    logging.info("Processing period: %s to %s", start_str, end_str)

    params = {
        "securityToken": TOKEN,
        "documentType": "A44",
        "in_Domain": ZONE,
        "out_Domain": ZONE,
        "periodStart": p_start,
        "periodEnd": p_end,
    }
    logging.info("Fetching price records from %s to %s", p_start, p_end)

    r = None
    for attempt in range(1, 4):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 200:
                break
            logging.warning("Retry attempt: HTTP %s, waiting %s seconds", r.status_code, attempt * 2)
            time.sleep(attempt * 2)
        except requests.RequestException as e:
            logging.warning("Network error on attempt: %s, waiting %s seconds", e, attempt * 2)
            time.sleep(attempt * 2)

    if r is not None and r.status_code == 200:
        root = ET.fromstring(r.content)
        ns = {"n": root.tag.split("}")[0].strip("{")}
        if not root.findall("n:TimeSeries", ns):
            reason = root.find(".//n:Reason/n:text", ns)
            msg = reason.text if reason is not None else "Unknown empty response"
            logging.error("No time series data found for %s to %s: %s", start_str, end_str, msg)
            failed_periods += 1
        else:
            file_name = f"FI_{p_start}_{p_end}.xml"
            out_path = out_dir / file_name
            out_path.write_bytes(r.content)
            logging.info("Saved payload to %s", file_name)
    else:
        status = r.status_code if r is not None else "No response"
        logging.error("Failed request for %s to %s with status: %s", start_str, end_str, status)
        failed_periods += 1

    # Throttle requests to respect API limits
    time.sleep(2.0)

    # Move to next month
    current_start = current_end

if failed_periods > 0:
    logging.error("Ingestion finished with %s failed periods", failed_periods)
    sys.exit(1)

logging.info("Completed ingestion pipeline successfully")
