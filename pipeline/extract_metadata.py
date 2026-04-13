import duckdb
import json
import os
from pathlib import Path
from datetime import datetime

# Configuration
PIPELINE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PIPELINE_DIR.parent
WEBSITE_DATA_DIR = PROJECT_ROOT.parent / "portfolio_website" / "src" / "data"
METADATA_FILE = WEBSITE_DATA_DIR / "catalog_metadata.json"

# Token from env or default
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

def extract_metadata():
    print(f"[{datetime.now()}] Connecting to MotherDuck...")
    con = duckdb.connect(f'md:?motherduck_token={MOTHERDUCK_TOKEN}')
    
    try:
        print(f"[{datetime.now()}] Fetching tables and columns from 'gold' schema...")
        
        # Query to get table, column, and basic stats
        query = """
        SELECT 
            table_name, 
            column_name, 
            data_type as type,
            count(*) over (partition by table_name, column_name) as total_rows
        FROM information_schema.columns 
        WHERE table_schema = 'gold'
        ORDER BY table_name, ordinal_position
        """
        
        df = con.execute(query).df()
        
        metadata = []
        
        for index, row in df.iterrows():
            table = row['table_name']
            column = row['column_name']
            col_type = row['type'].lower()
            
            # Map types to IDE types
            ide_type = 'text'
            if 'int' in col_type:
                ide_type = 'number'
            elif 'decimal' in col_type or 'double' in col_type or 'float' in col_type:
                ide_type = 'decimal'
            elif 'date' in col_type or 'timestamp' in col_type:
                ide_type = 'date'
                
            # Get samples and distinct count
            stats_query = f"SELECT count(DISTINCT {column}) as distinct_count, count({column}) as non_null FROM gold.{table}"
            stats = con.execute(stats_query).fetchone()
            
            sample_query = f"SELECT DISTINCT {column} FROM gold.{table} WHERE {column} IS NOT NULL LIMIT 20"
            samples = [str(s[0]) for s in con.execute(sample_query).fetchall()]
            
            metadata.append({
                "table_name": table,
                "column_name": column,
                "non_null": f"{stats[1]:,}",
                "distinct": f"{stats[0]:,}",
                "samples": samples,
                "type": ide_type
            })
            print(f"  - Processed {table}.{column}")

        # Fetch Table Previews (SELECT * LIMIT 100)
        print(f"[{datetime.now()}] Fetching real data previews (100 rows)...")
        table_previews = {}
        for table in df['table_name'].unique():
            preview_query = f"SELECT * FROM gold.{table} LIMIT 100"
            preview_df = con.execute(preview_query).df()
            # Convert to list of dicts
            table_previews[table] = preview_df.to_dict(orient='records')
            print(f"  - Captured preview for {table}")

        # Combined result
        result = {
            "metadata": metadata,
            "previews": table_previews,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        # Ensure directory exists
        os.makedirs(WEBSITE_DATA_DIR, exist_ok=True)
        
        with open(METADATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=4)
            
        print(f"[{datetime.now()}] REAL Metadata successfully exported to {METADATA_FILE}")

    finally:
        con.close()

if __name__ == "__main__":
    if not MOTHERDUCK_TOKEN:
        print("Error: MOTHERDUCK_TOKEN not found.")
    else:
        try:
            extract_metadata()
        except Exception as e:
            print(f"Extraction Error: {str(e)}")
