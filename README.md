# Data Warehouse - Sync Engine

A production-centric data warehouse focused on high-fidelity healthcare network synchronization. This module serves as the primary data hub for the Network Planner platform.

## Core Purpose
This module manages the storage and cloud distribution of professional network datasets:

1. **Warehouse Storage**: Managed via DuckDB (`warehouse.db`), containing 587k+ members with optimized geographic distributions.
2. **Cloud Distribution**: `gold_sync.py` manages the seamless integration with MotherDuck for global cloud accessibility.
3. **Standards**: Strict uppercase naming conventions and Pareto-distributed market weights are already baked into the synchronized gold layer.

## Project Structure
- **/duckdb**: Production database (`warehouse.db`).
- **/pipeline**: Core sync engine (`gold_sync.py`).
- **/data/gold**: Final Parquet exports.

## How to Sync
1. Install requirements:
   ```bash
   pip install -r requirements.txt
   ```
2. Execute MotherDuck upload:
   ```bash
   python pipeline/gold_sync.py
   ```

## Technical Note
This repository contains prepared high-fidelity data. Development scripts used for the initial population generation have been purged to maintain a clean, production-ready state.
