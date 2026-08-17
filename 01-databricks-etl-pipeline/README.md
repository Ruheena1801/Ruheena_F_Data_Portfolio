# Databricks Medallion ETL Pipeline

> **Portfolio sample project** demonstrating production-oriented Databricks data-engineering patterns. This repository contains representative sample code and does not contain employer or client proprietary code.

## Overview

This project demonstrates a scalable **Bronze → Silver → Gold** data pipeline using **Databricks, PySpark, Delta Lake, and AWS S3**. The sample focuses on incremental ingestion, schema evolution, data cleansing, data-quality validation, business-ready aggregation, and performance optimization.

The goal is to show how raw sales events can be ingested from cloud storage, standardized into a trusted Silver layer, and transformed into Gold datasets that are ready for analytics and reporting.

## Architecture

```text
AWS S3 Raw Files
      |
      v
Databricks Auto Loader
      |
      v
Bronze Delta Layer
(raw + ingestion metadata)
      |
      v
Silver Delta Layer
(cleaned + standardized + deduplicated + validated)
      |
      v
Gold Delta Layer
(business-ready daily sales metrics)
```

## Pipeline Layers

### Bronze — Incremental ingestion

The Bronze layer preserves incoming records while adding ingestion metadata.

Key patterns demonstrated:
- Databricks **Auto Loader (`cloudFiles`)** for incremental file ingestion
- Schema inference and **schema evolution**
- `schemaLocation` for persisted schema metadata
- Streaming **checkpoints** for reliable incremental processing
- Source-file and ingestion-timestamp metadata
- Delta Lake storage for durable raw history

### Silver — Cleansing and standardization

The Silver layer converts raw records into trusted, reusable data.

Transformations include:
- Standardizing timestamps and dates
- Casting numeric values to consistent data types
- Trimming and normalizing customer attributes
- Filtering invalid/null business keys
- Deduplicating by `order_id`
- Watermarking for streaming duplicate control
- Writing curated records to Delta Lake

### Gold — Analytics-ready data

The Gold layer provides a simplified business dataset suitable for BI and downstream analytics.

The sample creates daily metrics including:
- Distinct order count
- Total sales
- Average order value

This illustrates how curated Silver records can be transformed into reusable business-facing data products.

## Data Quality

The project includes reusable validation checks for:
- Null business keys
- Missing event dates
- Negative sales amounts
- Duplicate order IDs

In a larger production implementation, these checks could be extended with tools such as **Pytest, dbt tests, or Great Expectations** and integrated into CI/CD workflows.

## Incremental Processing & Reliability

The pipeline uses:
- Auto Loader for newly arriving files
- Delta Lake for reliable storage
- Streaming checkpoints to track progress
- Schema evolution for compatible source changes
- `availableNow` triggers for incremental micro-batch processing

These patterns help avoid reprocessing previously consumed data and support recoverable pipeline execution.

## Performance Optimization

The sample includes **Delta Lake `OPTIMIZE` and Z-Ordering** on frequently accessed keys.

Additional production tuning patterns may include:
- Partition pruning
- Broadcast joins for small dimension datasets
- Adaptive Query Execution (AQE)
- Caching reused DataFrames
- Shuffle-partition tuning

## Technology Stack

- Databricks
- Apache Spark / PySpark
- Spark SQL
- Delta Lake
- Databricks Auto Loader
- AWS S3
- Structured Streaming
- Python

## Repository Files

- [`databricks_etl_notebook.py`](./databricks_etl_notebook.py) — Bronze, Silver, Gold pipeline implementation
- [`transformations.sql`](./transformations.sql) — example Delta SQL table definition

## What This Project Demonstrates

This sample highlights practical data-engineering concepts including:

- Medallion Architecture
- Incremental cloud ingestion
- Schema evolution
- Streaming checkpoints
- Data cleansing and deduplication
- Data-quality validation
- Delta Lake optimization
- Analytics-ready data modeling
- Production-oriented pipeline design

---

This project is intended as a portfolio demonstration of data-engineering patterns and contains only synthetic/example paths and logic.
