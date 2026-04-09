import duckdb
import os
import pandas as pd
from datetime import datetime
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PIPELINE_DIR.parent
DB_PATH = PROJECT_ROOT / "duckdb" / "warehouse.db"
DATA_DIR = PROJECT_ROOT / "data"

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

def init_folders():
    for layer in ["bronze", "silver", "gold"]:
        os.makedirs(DATA_DIR / layer, exist_ok=True)
    os.makedirs(PROJECT_ROOT / "pipeline", exist_ok=True)
    os.makedirs(PROJECT_ROOT / "duckdb", exist_ok=True)

def sync_to_motherduck():
    if not MOTHERDUCK_TOKEN:
        return
    con = duckdb.connect(f'md:?motherduck_token={MOTHERDUCK_TOKEN}')
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        abs_db_path = DB_PATH.resolve()
        con.execute(f"ATTACH '{abs_db_path}' AS local_db (READ_ONLY)")
        tables = ["members", "providers"]
        for table in tables:
            con.execute(f"CREATE OR REPLACE TABLE gold.{table} AS SELECT * FROM local_db.gold.{table}")
        con.execute("DETACH local_db")
    finally:
        con.close()

def export_gold_to_parquet():
    if not DB_PATH.exists():
        return
    con = duckdb.connect(str(DB_PATH))
    try:
        gold_dir = DATA_DIR / "gold"
        tables = ["members", "providers"]
        for table in tables:
            output_file = gold_dir / f"{table}.parquet"
            con.execute(f"COPY gold.{table} TO '{output_file}' (FORMAT PARQUET)")
    finally:
        con.close()

if __name__ == "__main__":
    init_folders()
    try:
        export_gold_to_parquet()
        sync_to_motherduck()
    except:
        pass
