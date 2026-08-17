# AWS Glue Data Pipeline

> **Portfolio sample:** This project demonstrates a representative AWS Glue ETL pattern using sample infrastructure and placeholder credentials. It does not contain employer/client code or proprietary data.

## Goal

Build an incremental data pipeline that extracts changed order records from PostgreSQL/RDS, transforms and validates them with PySpark in AWS Glue, and writes analytics-ready Parquet data to Amazon S3.

## Architecture

```text
PostgreSQL / Amazon RDS
        |
        v
Incremental JDBC Extract
        |
        v
AWS Glue + PySpark
        |
        +--> Standardization
        +--> Deduplication
        +--> Data Quality Checks
        |
        v
Partitioned Parquet on Amazon S3
        |
        v
Downstream Warehouse / Analytics Layer
```

## What the Sample Demonstrates

- Incremental extraction using an `updated_at` watermark
- JDBC-based ingestion from PostgreSQL/RDS
- PySpark transformations for type casting and standardization
- Duplicate, null-key, and negative-value validation
- Partitioned Parquet output by `event_date`
- Watermark handoff for the next scheduled run
- Clear separation between source, transformation, validation, and target stages

## Incremental Processing

The job accepts a `LAST_WATERMARK` parameter and filters the source query to records where `updated_at` is newer than the last successful run.

After processing, the job calculates the latest `event_ts` as the next watermark. In a production environment, this value would typically be persisted in a control table or metadata service.

## Data Quality Guardrails

Before writing output, the sample checks for:

- Null `order_id` values
- Negative order amounts
- Duplicate `order_id` values

Records are also standardized by trimming/capitalizing status values and casting numeric and timestamp fields.

## Storage Design

Curated data is written to S3 in **Parquet** format and partitioned by `event_date`. This supports efficient downstream reads and partition pruning for analytics workloads.

## Production Considerations

For a real production deployment, the sample would be extended with:

- AWS Secrets Manager or Glue Connections for credentials
- Centralized watermark/control-table persistence
- CloudWatch logging and alerting
- Retry/error-handling strategy
- Orchestration through Airflow, Step Functions, or scheduled Glue workflows
- A downstream Snowflake/Redshift load or external-table integration

## Technologies

**AWS Glue | PySpark | Python | Amazon S3 | PostgreSQL/RDS | Parquet | Incremental ETL | Data Quality**

## Repository Contents

- `README.md` — architecture and design notes
- `glue_job_script.py` — sample incremental AWS Glue/PySpark ETL job
