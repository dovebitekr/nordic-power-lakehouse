import os
import requests
from dotenv import load_dotenv
import json
import time
from pathlib import Path
import argparse
from datetime import datetime
from dateutil.relativedelta import relativedelta

load_dotenv()
API_KEY = os.getenv("FINGRID_API_KEY")
if not API_KEY:
    raise ValueError("FINGRID_API_KEY is not configured in the .env file")

DATASETS = {
    "wind_generation": 75, #interval:15 min
    "nuclear_generation": 188, #interval:3 min
    "hydro_generation": 191, #interval:3 min
    "total_consumption": 124 #electricity consumption in Finland - 15 min
}
HEADERS = {
    "x-api-key": API_KEY,
    "Accept": "application/json"
}

parser = argparse.ArgumentParser(description="Fetch Fingrid datasets")
parser.add_argument(
    "--start",
    type=str,
    default="2026-10-22",
    help="Start date in YYYY-MM-DD format (default: 2026-10-22)",
)
parser.add_argument(
    "--end",
    type=str,
    default="2026-10-25",
    help="End date in YYYY-MM-DD format (default: 2026-10-25)",
)
args = parser.parse_args()

OUTPUT_DIR = Path("data/bronze/fingrid")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
# Parse start and end dates
current_start = datetime.strptime(args.start, "%Y-%m-%d")
global_end = datetime.strptime(args.end, "%Y-%m-%d")

# Loop month by month
while current_start < global_end:
  current_end = min(current_start + relativedelta(months=1), global_end)
  start_str = current_start.strftime("%Y-%m-%d")
  end_str = current_end.strftime("%Y-%m-%d")
  print(f"\n--- Period: {start_str} to {end_str} ---")
  params = {
      "startTime": f"{start_str}T00:00:00.000Z",
      "endTime": f"{end_str}T00:00:00.000Z",
      "format": "json",
      "pageSize": 20000,
  }
  for metric_name, dataset_id in DATASETS.items():
    print(f"Fetching '{metric_name}' (ID: {dataset_id})...")
    endpoint_url = f"https://data.fingrid.fi/api/datasets/{dataset_id}/data"
    response = requests.get(endpoint_url, headers=HEADERS, params=params)
    if response.status_code == 200:
      payload = response.json()
      record_count = len(payload.get("data", []))
      total_count = payload.get("pagination", {}).get("total", 0)
      print(f"  [Success] {record_count}/{total_count} records.")
      file_path = OUTPUT_DIR / f"{metric_name}_{start_str}_{end_str}.json"
      with open(file_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
      print(f"  [Saved] {file_path.name}")
    else:
      print(f"  [Failed] Status: {response.status_code}")
      print(f"  Response: {response.text}")
    # Throttle requests to respect API rate limits
    time.sleep(2.5)
  # Move to next month
  current_start = current_end