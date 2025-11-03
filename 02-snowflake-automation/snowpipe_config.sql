-- Snowpipe configuration (sample)
CREATE OR REPLACE STAGE raw_sales_s3
  URL='s3://bucket/raw/sales/'
  FILE_FORMAT=(TYPE=JSON);

CREATE OR REPLACE TABLE raw_sales (record VARIANT);

CREATE OR REPLACE PIPE raw_sales_pipe AS
  COPY INTO raw_sales
  FROM @raw_sales_s3
  FILE_FORMAT=(TYPE=JSON)
  ON_ERROR='CONTINUE';
