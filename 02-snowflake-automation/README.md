# Snowflake Automation Pipeline

> **Portfolio sample:** This project demonstrates reusable Snowflake and Airflow engineering patterns using synthetic/example data. It is not employer or client source code.

## Goal
Automate ingestion from Amazon S3 into Snowflake, standardize semi-structured data, incrementally merge records into a curated analytics table, validate data quality, and orchestrate the workflow with Airflow.

## Architecture

```text
Amazon S3
   |
   v
Snowflake Stage + Snowpipe
   |
   v
RAW_SALES (VARIANT)
   |
   v
STG_SALES
   |  parsing / type conversion / standardization
   v
MERGE / incremental upsert
   |
   v
SALES_CURATED
   |
   +--> Data-quality validation
   |
   v
MART_DAILY_SALES
```

## What This Project Demonstrates

### 1. Automated ingestion
- External S3 stage and JSON file format
- Snowpipe-based ingestion into a raw Snowflake table
- Source-file and ingestion-timestamp metadata for traceability

### 2. Layered transformation
- **Raw:** preserves semi-structured source records as `VARIANT`
- **Staging:** parses JSON fields, standardizes email values, and safely converts data types
- **Curated:** maintains one durable record per `order_id`
- **Mart:** produces daily sales metrics for downstream reporting

### 3. Incremental processing
The curated table uses a Snowflake `MERGE` pattern to support repeatable incremental loads and updates. A `ROW_NUMBER()` rule selects the latest source record for each business key before the upsert.

### 4. Data quality and reconciliation
The workflow includes checks for:
- Null business keys
- Duplicate `order_id` values
- Invalid/negative amounts
- Missing daily data
- Row-count/reconciliation queries between processing layers

The Airflow validation task fails the workflow when critical quality rules are violated.

### 5. Orchestration and operational reliability
The included Airflow DAG demonstrates:
- Scheduled execution
- Retry handling
- Transformation followed by validation
- Failure propagation through assertions
- A reusable Snowflake connection pattern

## Technologies

- Snowflake
- Snowpipe
- Amazon S3
- SQL
- Python
- Apache Airflow
- Semi-structured JSON / `VARIANT`
- Incremental `MERGE`
- Data-quality validation

## Files

- `snowpipe_config.sql` — Snowflake stage, Snowpipe, raw/staging/curated/mart objects, incremental merge, and validation SQL
- `airflow_dag_example.py` — Airflow orchestration for transformation and quality validation

## Engineering Concepts Highlighted

- ETL/ELT automation
- Raw → staging → curated → mart design
- Incremental loads and upserts
- Deduplication using business keys
- Source metadata and lineage support
- Data-quality guardrails
- Workflow retries and failure handling
- Analytics-ready data modeling
