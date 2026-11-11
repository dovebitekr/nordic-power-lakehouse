import logging
import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

user = os.getenv("POSTGRES_USER", "lakehouse")
password = os.getenv("POSTGRES_PASSWORD", "lakehouse_pass")
host = os.getenv("POSTGRES_HOST", "localhost")
port = os.getenv("POSTGRES_PORT", "5433")
dbname = os.getenv("POSTGRES_DB", "nordic_power")

gold_path = BASE_DIR / "data/gold/power_market_15min"
df = pd.read_parquet(gold_path).sort_values("start_time_utc").reset_index(drop=True)

db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(db_url)

table_name = "power_market_15min"
logging.info("Loading %s rows into PostgreSQL table %s", len(df), table_name)

df.to_sql(
    name=table_name,
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=5000,
    method="multi",
)

with engine.begin() as conn:
    conn.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY (start_time_utc);"))
    row_count, avg_price = conn.execute(
        text(f"SELECT COUNT(*), ROUND(AVG(price_eur_per_mwh)::numeric, 2) FROM {table_name};")
    ).fetchone()

logging.info(
    "PostgreSQL load verified: %s rows in %s (average price: %s EUR/Mwh)",
    row_count,
    table_name,
    avg_price
)