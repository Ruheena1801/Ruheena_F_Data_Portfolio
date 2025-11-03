-- Example Delta table DDL
CREATE TABLE IF NOT EXISTS sales_curated (
  order_id STRING,
  customer_email STRING,
  amount DOUBLE,
  event_date DATE
)
USING DELTA
PARTITIONED BY (event_date);
