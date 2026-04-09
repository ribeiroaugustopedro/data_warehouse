# Professional Data Warehouse (Portfolio Project)

A production-grade local data warehouse pipeline using **DuckDB**, **dbt**, and **MotherDuck**, implementing a **Medallion Architecture** (Bronze / Silver / Gold) with an emphasis on **Public-Safe Anonymization**.

---

## 🏗️ Medallion Architecture & Anonymization Strategy

This pipeline transforms sensitive project data into a structurally faithful, public-ready portfolio warehouse.

1. **Source (Sensitive)** ➡️ `warehouse.db` (Local initial state).
2. **Bronze (Anonymized Raw)** ➡️ `data/bronze/*.parquet`.
   *   **Hashing**: Primary and Foreign Keys are deterministically hashed (SHA-256 with salt) to preserve relational integrity.
   *   **Date Shifting**: Temporal patterns are maintained but shifted per entity for privacy.
   *   **Synthetic Data**: Names, emails, and sensitive strings are replaced using **Faker**.
   *   **Numeric Scaling**: Financial metrics and KPIs are scaled (0.9x-1.1x) with added noise.
3. **Silver (Standardized dbt)** ➡️ DuckDB `silver` schema.
   *   Data cleaning, casting, and renaming to public-safe conventions.
4. **Gold (Curated dbt)** ➡️ DuckDB `gold` schema.
   *   Final analytical models supporting downstream apps and dashboards.
5. **MotherDuck (Cloud)** ➡️ Final publishing layer for the portfolio.

## 🛠️ Tech Stack
-   **Engine**: DuckDB (Local OLAP)
-   **Transformations**: dbt (Data Build Tool)
-   **Anonymization**: Python (Pandas/Faker/Hashlib)
-   **Cloud Hosting**: MotherDuck

## 🚀 Execution Flow

1. **Anonymization & Ingestion**:
   ```bash
   python v_01_ingest_and_anonymize.py
   ```
2. **dbt Transformations**:
   ```bash
   cd dbt
   dbt run --profiles-dir .
   ```
3. **MotherDuck Sync**:
   ```bash
   python migrate_to_md.py
   ```

## 📂 Project Structure
-   `/data/bronze`: Anonymized parquet files (The "Public Raw" source).
-   `/dbt`: dbt project (staging/marts models).
-   `/duckdb`: Local warehouse storage (`warehouse.db`).
-   `/utils`: Python utilities (anonymizer, profiler).

---
> [!IMPORTANT]
> This project is designed to be **Public-Safe**. No raw PII or sensitive business financials are stored in the final repository or MotherDuck instance.
