import os
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
import argparse
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

TOKEN = os.getenv("ENTSOE_API_KEY")
if not TOKEN:
    raise ValueError("ENTSOE_API_KEY is missing in .env file.")

parser = argparse.ArgumentParser(description="Fetch ENTSO-E Day-Ahead Prices for Finland")
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

out_dir = BASE_DIR / "data/bronze/entsoe/day_ahead_prices/FI"
out_dir.mkdir(parents=True, exist_ok=True)
ZONE = "10YFI-1--------U"  # Finland bidding zone
url = "https://web-api.tp.entsoe.eu/api"
# Parse start and end dates
current_start = datetime.strptime(args.start, "%Y-%m-%d")
global_end = datetime.strptime(args.end, "%Y-%m-%d")
# Loop month by month
while current_start < global_end:
  current_end = min(current_start + relativedelta(months=1), global_end)
  start_str = current_start.strftime("%Y-%m-%d")
  end_str = current_end.strftime("%Y-%m-%d")
  p_start = current_start.strftime("%Y%m%d0000")
  p_end = current_end.strftime("%Y%m%d0000")
  print(f"\n--- Period: {start_str} to {end_str} ---")
  params = {
      "securityToken": TOKEN,
      "documentType": "A44",
      "in_Domain": ZONE,
      "out_Domain": ZONE,
      "periodStart": p_start,
      "periodEnd": p_end,
  }
  print(f"Fetching ENTSO-E prices ({p_start} -> {p_end})...")
  r = requests.get(url, params=params, timeout=30)
  r.raise_for_status()
  root = ET.fromstring(r.content)
  ns = {"n": root.tag.split("}")[0].strip("{")}
  if not root.findall("n:TimeSeries", ns):
    reason = root.find(".//n:Reason/n:text", ns)
    msg = reason.text if reason is not None else "Unknown empty response"
    print(f"  [Warning] No data: {msg}")
  else:
    file_name = f"FI_{p_start}_{p_end}.xml"
    out_path = out_dir / file_name
    out_path.write_bytes(r.content)
    print(f"  [Saved] {file_name}")
  # Throttle requests to respect API limits
  time.sleep(2.0)
  # Move to next month
  current_start = current_end
print("\nENTSO-E ingestion pipeline completed.")