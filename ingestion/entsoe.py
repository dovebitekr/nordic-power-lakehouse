import os
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

TOKEN = os.getenv("ENTSOE_API_KEY")
if not TOKEN:
    raise ValueError("ENTSOE_API_KEY is missing in .env file.")

ZONE = "10YFI-1--------U"  # Finland
PERIOD_START = "202610220000"  # Match Fingrid UTC start date
PERIOD_END = "202610250000"    # Match Fingrid UTC end date

params = {
    "securityToken": TOKEN,
    "documentType": "A44",
    "in_Domain": ZONE,
    "out_Domain": ZONE,
    "periodStart": PERIOD_START,
    "periodEnd": PERIOD_END,
}

# Fetch XML from ENTSO-E API
url = "https://web-api.tp.entsoe.eu/api"
r = requests.get(url, params=params, timeout=30)
r.raise_for_status()

# Ensure XML contains TimeSeries data
root = ET.fromstring(r.content)
ns = {"n": root.tag.split("}")[0].strip("{")}

if not root.findall("n:TimeSeries", ns):
    reason = root.find(".//n:Reason/n:text", ns)
    msg = reason.text if reason is not None else "Unknown empty response"
    raise RuntimeError(f"ENTSO-E returned no data: {msg}")

# Save raw XML to Bronze layer
out_dir = BASE_DIR / "data/bronze/entsoe/day_ahead_prices/FI"
out_dir.mkdir(parents=True, exist_ok=True)

file_name = f"FI_{PERIOD_START}_{PERIOD_END}.xml"
out_path = out_dir / file_name

out_path.write_bytes(r.content)
print(f"Saved Bronze XML successfully: {out_path}")