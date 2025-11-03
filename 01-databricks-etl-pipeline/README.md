# Databricks ETL Pipeline

**Goal:** Ingest raw sales data, apply cleansing, and produce a curated Delta table optimized for BI.

**Highlights**
- Handles schema evolution with Delta Lake
- Partitioning by event_date for efficient reads
- Unit tests in Python for core transforms (example shown inline)

**Tech:** Databricks, PySpark, Delta Lake, Python

## Notebook Outline
1. Read raw data from S3
2. Transform (trim, dedupe, cast types)
3. Write to Delta with merge for upserts
4. Optimize & VACUUM
