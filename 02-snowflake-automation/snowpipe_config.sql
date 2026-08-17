-- Snowflake Automation Portfolio Sample
-- Demonstrates S3 ingestion with Snowpipe, raw-to-curated transformation,
-- incremental MERGE logic, and data-quality validation.

-- ---------------------------------------------------------------------------
-- File format and external stage
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT sales_json_format
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE STAGE raw_sales_s3
  URL = 's3://sample-bucket/raw/sales/'
  FILE_FORMAT = sales_json_format;

-- ---------------------------------------------------------------------------
-- RAW layer
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE raw_sales (
  record VARIANT,
  source_file STRING,
  loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PIPE raw_sales_pipe
  AUTO_INGEST = TRUE
AS
COPY INTO raw_sales (record, source_file)
FROM (
  SELECT
    $1,
    METADATA$FILENAME
  FROM @raw_sales_s3
)
FILE_FORMAT = (FORMAT_NAME = sales_json_format)
ON_ERROR = 'CONTINUE';

-- ---------------------------------------------------------------------------
-- STAGING layer: parse and standardize semi-structured JSON
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW stg_sales AS
SELECT
  record:order_id::STRING AS order_id,
  LOWER(TRIM(record:customer_email::STRING)) AS customer_email,
  TRY_TO_DOUBLE(record:amount::STRING) AS amount,
  TRY_TO_TIMESTAMP_NTZ(record:timestamp::STRING) AS event_ts,
  TO_DATE(TRY_TO_TIMESTAMP_NTZ(record:timestamp::STRING)) AS event_date,
  source_file,
  loaded_at
FROM raw_sales;

-- ---------------------------------------------------------------------------
-- CURATED layer: durable target table for analytics
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sales_curated (
  order_id STRING,
  customer_email STRING,
  amount NUMBER(18,2),
  event_ts TIMESTAMP_NTZ,
  event_date DATE,
  source_file STRING,
  created_at TIMESTAMP_NTZ,
  updated_at TIMESTAMP_NTZ
);

-- Incremental upsert pattern. In production, this statement can be called
-- from Airflow/dbt after each ingestion cycle.
MERGE INTO sales_curated tgt
USING (
  SELECT
    order_id,
    customer_email,
    amount,
    event_ts,
    event_date,
    source_file,
    loaded_at,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY loaded_at DESC
    ) AS rn
  FROM stg_sales
  WHERE order_id IS NOT NULL
    AND amount IS NOT NULL
    AND amount >= 0
) src
ON tgt.order_id = src.order_id
WHEN MATCHED AND src.rn = 1 THEN UPDATE SET
  customer_email = src.customer_email,
  amount = src.amount,
  event_ts = src.event_ts,
  event_date = src.event_date,
  source_file = src.source_file,
  updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND src.rn = 1 THEN INSERT (
  order_id,
  customer_email,
  amount,
  event_ts,
  event_date,
  source_file,
  created_at,
  updated_at
) VALUES (
  src.order_id,
  src.customer_email,
  src.amount,
  src.event_ts,
  src.event_date,
  src.source_file,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- MART layer
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart_daily_sales AS
SELECT
  event_date,
  COUNT(DISTINCT order_id) AS order_count,
  SUM(amount) AS total_sales,
  AVG(amount) AS avg_order_value
FROM sales_curated
GROUP BY event_date;

-- ---------------------------------------------------------------------------
-- Data-quality / reconciliation queries
-- ---------------------------------------------------------------------------
-- Null key check
SELECT COUNT(*) AS null_order_ids
FROM sales_curated
WHERE order_id IS NULL;

-- Duplicate key check
SELECT order_id, COUNT(*) AS duplicate_count
FROM sales_curated
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Invalid amount check
SELECT COUNT(*) AS invalid_amount_rows
FROM sales_curated
WHERE amount < 0;

-- Daily reconciliation between staging and curated layers
SELECT
  event_date,
  COUNT(*) AS staged_rows
FROM stg_sales
GROUP BY event_date
ORDER BY event_date DESC;
