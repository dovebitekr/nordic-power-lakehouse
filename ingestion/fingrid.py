import os
import requests
from dotenv import load_dotenv
import json
import time
from pathlib import Path

load_dotenv()
API_KEY = os.getenv("FINGRID_API_KEY")
if not API_KEY:
    raise ValueError("FINGRID_API_KEY is not configured in the .env file")

DATASETS = {
    "wind_generation": 75,
    "nuclear_generation": 188,
    "solar_generation": 248,
    "hydro_generation": 191,
    "total_consumption": 124
}
HEADERS = {
    "x-api-key": API_KEY,
    "Accept": "application/json"
}

params = {
    "startTime": "2026-10-22T00:00:00.000Z",
    "endTime": "2026-10-25T00:00:00.000Z",
    "format": "json",
    "pageSize": 300
}

OUTPUT_DIR = Path("data/bronze/fingrid")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

for metric_name, dataset_id in DATASETS.items():
    print(f"Fetching '{metric_name}' (Dataset ID: {dataset_id})...")
    endpoint_url = f"https://data.fingrid.fi/api/datasets/{dataset_id}/data"

    response = requests.get(endpoint_url, headers=HEADERS, params=params)
    if response.status_code == 200:
        payload = response.json()
        record_count = len(payload.get("data", []))
        total_count = payload.get("pagination", {}).get("total", 0)
        print(f" [Success] Retrieved {record_count}/{total_count} records.")

        file_path = OUTPUT_DIR / f"{metric_name}_sample.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"  [Saved] {file_path}")
    else:
        print(f"  [Failed] Status: {response.status_code}")
        print(f". Response: {response.text}")

    #Throttle requests to respect FINGRID API rate limits (min 2.0s gap)
    time.sleep(2.5)
print("\nIngestion pipeline execution completed.")