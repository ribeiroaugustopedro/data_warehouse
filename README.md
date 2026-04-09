# Data Warehouse - Portfolio Project

A production-grade local data warehouse pipeline using DuckDB and MotherDuck, implementing a Medallion Architecture (Bronze, Silver, and Gold layers) with a focus on public-ready data anonymization.

## Architecture and Anonymization Strategy

This pipeline transforms project data into a structurally faithful, public-ready warehouse while ensuring privacy and reproducibility.

1. **Source Layer**: Local initial state.
2. **Bronze Layer**: Raw data anonymized using hashing for relational integrity and synthetic data generation for PII removal.
3. **Silver Layer**: Standardized and cleaned data processed via SQL/DuckDB.
4. **Gold Layer**: Curated analytical models optimized for reporting and downstream applications.
5. **MotherDuck Sync**: Final publishing layer for cloud-based accessibility.

## Tech Stack
- **Database Engine**: DuckDB
- **Cloud Interface**: MotherDuck
- **Processing**: Python (Pandas, PyArrow)
- **Data Format**: Parquet

## Project Structure
- **/data/gold**: Final analytical Parquet files.
- **/duckdb**: Local warehouse database storage.
- **/pipeline**: Core synchronization scripts (e.g., gold_sync.py).

## Setup and Execution
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Run the synchronization pipeline:
   ```bash
   python pipeline/gold_sync.py
   ```

## Disclaimer
This project is designed for public demonstration. No private PII or sensitive business financials are stored in this repository or the connected MotherDuck instance.
