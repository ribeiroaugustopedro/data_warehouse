import duckdb
import os
import pandas as pd
from datetime import datetime
from pathlib import Path

# Configuration - Dynamic paths based on file location
PIPELINE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PIPELINE_DIR.parent
DB_PATH = PROJECT_ROOT / "duckdb" / "warehouse.db"
DATA_DIR = PROJECT_ROOT / "data"

# Recommend moving this to environment variables for security
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InJpYmVpcm9hdWd1c3RvcGVkcm9AZ21haWwuY29tIiwibWRSZWdpb24iOiJhd3MtdXMtZWFzdC0xIiwic2Vzc2lvbiI6InJpYmVpcm9hdWd1c3RvcGVkcm8uZ21haWwuY29tIiwicGF0IjoiUXZaOWowb2U0WlVSUlowNW84QmdXaTF3ek94LTduVFE3TnkwaDNOMUQ1NCIsInVzZXJJZCI6IjlkZjZmMmVjLTNjYWYtNDRmZC05NzA0LWU4OTdjMDcyYjExZSIsImlzcyI6Im1kX3BhdCIsInJlYWRPbmx5IjpmYWxzZSwidG9rZW5UeXBlIjoicmVhZF93cml0ZSIsImlhdCI6MTc3NTQ4NjE0MX0.HMNMkFOiAT0TOqI6XsddmcBvaEE-AZDIjxCy4b6YDhc")

def init_folders():
    """Initialize necessary directory structure."""
    for layer in ["bronze", "silver", "gold"]:
        os.makedirs(DATA_DIR / layer, exist_ok=True)
    os.makedirs(PROJECT_ROOT / "pipeline", exist_ok=True)
    os.makedirs(PROJECT_ROOT / "duckdb", exist_ok=True)

def sync_to_motherduck():
    """Synchronize local gold tables to MotherDuck cloud instance."""
    print(f"[{datetime.now()}] Syncing Local Gold Layer to MotherDuck...")
    con = duckdb.connect(f'md:?motherduck_token={MOTHERDUCK_TOKEN}')
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.execute("DROP TABLE IF EXISTS gold.members")
        
        # Attach local DB using absolute path
        abs_db_path = DB_PATH.resolve()
        con.execute(f"ATTACH '{abs_db_path}' AS local_db (READ_ONLY)")
        
        # Sync Gold Tables
        tables = ["users", "providers"]
        for table in tables:
            con.execute(f"CREATE OR REPLACE TABLE gold.{table} AS SELECT * FROM local_db.gold.{table}")
            print(f"  - Table gold.{table} synced successfully.")
            
        con.execute("DETACH local_db")
    finally:
        con.close()
    print(f"[{datetime.now()}] MotherDuck sync completed.")

def export_gold_to_parquet():
    """Export gold tables from local DuckDB to Parquet for portability."""
    print(f"[{datetime.now()}] Exporting Gold tables to Parquet files...")
    if not DB_PATH.exists():
        print(f"  - Warning: Local database not found at {DB_PATH}")
        return

    con = duckdb.connect(str(DB_PATH))
    try:
        gold_dir = DATA_DIR / "gold"
        tables = ["users", "providers"]
        for table in tables:
            output_file = gold_dir / f"{table}.parquet"
            con.execute(f"COPY gold.{table} TO '{output_file}' (FORMAT PARQUET)")
            print(f"  - Exported gold.{table} to {output_file.name}")
    finally:
        con.close()

if __name__ == "__main__":
    init_folders()
    try:
        export_gold_to_parquet()
        sync_to_motherduck()
        print("\nGold Sync completed successfully.")
    except Exception as e:
        print(f"\nGold Sync Error: {str(e)}")
