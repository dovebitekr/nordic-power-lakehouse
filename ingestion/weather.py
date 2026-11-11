import argparse
import json
import logging
from pathlib import Path
import sys
import time
import requests
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

BASE_DIR = Path(__file__).resolve().parent.parent

# Load configuration from central YAML file
with open(BASE_DIR / "common/config.yaml", "r", encoding="utf-8") as cfg_file:
    CONFIG = yaml.safe_load(cfg_file)

WEATHER_CFG = CONFIG["weather"]
BASE_URL = WEATHER_CFG["base_url"]

parser = argparse.ArgumentParser(description="Fetch Finnish weather observations from Open-Meteo")
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
parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Re-download files even if they already exist locally",
)
args = parser.parse_args()

OUTPUT_DIR = BASE_DIR / "data/bronze/weather"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

stations = [
    {
        "name": "helsinki_temp",
        "station_id": WEATHER_CFG["demand_hub"]["station_id"],
        "latitude": WEATHER_CFG["demand_hub"]["latitude"],
        "longitude": WEATHER_CFG["demand_hub"]["longitude"],
        "hourly": "temperature_2m",
    },
    {
        "name": "vaasa_wind",
        "station_id": WEATHER_CFG["wind_hub"]["station_id"],
        "latitude": WEATHER_CFG["wind_hub"]["latitude"],
        "longitude": WEATHER_CFG["wind_hub"]["longitude"],
        "hourly": "wind_speed_10m",
    },
]

failed_requests = 0

for station in stations:
    file_path = OUTPUT_DIR / f"{station['name']}_{args.start}_{args.end}.json"
    if file_path.exists() and file_path.stat().st_size > 0 and not args.overwrite:
        logging.info("Skipping existing weather file: %s", file_path.name)
        continue

    params = {
        "latitude": station["latitude"],
        "longitude": station["longitude"],
        "start_date": args.start,
        "end_date": args.end,
        "hourly": station["hourly"],
        "wind_speed_unit": "ms",
        "timezone": "UTC",
    }

    logging.info(
        "Fetching Open-Meteo archive for %s (%s) from %s to %s",
        station["name"],
        station["station_id"],
        args.start,
        args.end,
    )

    response = None
    for attempt in range(1, 4):
        try:
            response = requests.get(BASE_URL, params=params, timeout=30)
            if response.status_code == 200:
                break
            logging.warning(
                "Retry attempt for %s: HTTP %s, waiting %s seconds",
                station["name"],
                response.status_code,
                attempt * 2,
            )
            time.sleep(attempt * 2)
        except requests.RequestException as exc:
            logging.warning(
                "Network error for %s: %s, waiting %s seconds",
                station["name"],
                exc,
                attempt * 2,
            )
            time.sleep(attempt * 2)

    if response is not None and response.status_code == 200:
        payload = response.json()
        hourly_times = payload.get("hourly", {}).get("time", [])
        if not hourly_times:
            logging.error("Received empty hourly series for %s", station["name"])
            failed_requests += 1
        else:
            with open(file_path, "w", encoding="utf-8") as out_file:
                json.dump(payload, out_file, indent=2)
            logging.info(
                "Saved %s hourly records to %s",
                len(hourly_times),
                file_path.name,
            )
    else:
        status = response.status_code if response is not None else "No response"
        text = response.text if response is not None else "Connection failed"
        logging.error(
            "Failed weather request for %s status: %s response: %s",
            station["name"],
            status,
            text,
        )
        failed_requests += 1

    time.sleep(1.0)

if failed_requests > 0:
    logging.error("Weather ingestion finished with %s failed requests", failed_requests)
    sys.exit(1)

logging.info("Completed weather ingestion successfully")
