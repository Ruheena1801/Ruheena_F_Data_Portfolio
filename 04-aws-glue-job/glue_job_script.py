# AWS Glue ETL Pipeline (portfolio sample)
# Demonstrates incremental RDS extraction, PySpark transformations,
# data-quality checks, and partitioned Parquet output to S3.

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# Job parameters
# LAST_WATERMARK represents the latest successfully processed timestamp.
# -----------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "LAST_WATERMARK"])
last_watermark = args["LAST_WATERMARK"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------
# SOURCE: Incremental extraction from PostgreSQL/RDS
# Credentials are placeholders for this portfolio sample. In production,
# use AWS Secrets Manager / Glue Connections instead of hard-coded values.
# -----------------------------------------------------------------------------
jdbc_url = "jdbc:postgresql://sample-rds-endpoint:5432/appdb"
connection_properties = {
    "user": "sample_user",
    "password": "sample_password",
    "driver": "org.postgresql.Driver",
}

incremental_query = f"""
(
    SELECT
        order_id,
        customer_id,
        amount,
        status,
        created_at,
        updated_at
    FROM public.orders
    WHERE updated_at > TIMESTAMP '{last_watermark}'
) AS incremental_orders
"""

source_df = spark.read.jdbc(
    url=jdbc_url,
    table=incremental_query,
    properties=connection_properties,
)

# -----------------------------------------------------------------------------
# TRANSFORM: Standardize and deduplicate
# -----------------------------------------------------------------------------
curated_df = (
    source_df
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("status", F.upper(F.trim(F.col("status"))))
    .withColumn("event_ts", F.to_timestamp("updated_at"))
    .withColumn("event_date", F.to_date("event_ts"))
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("amount").isNotNull())
    .filter(F.col("amount") >= 0)
    .dropDuplicates(["order_id"])
    .withColumn("processed_at", F.current_timestamp())
)

# -----------------------------------------------------------------------------
# DATA QUALITY: Lightweight guardrails
# -----------------------------------------------------------------------------
null_order_ids = curated_df.filter(F.col("order_id").isNull()).count()
negative_amounts = curated_df.filter(F.col("amount") < 0).count()
duplicate_orders = (
    curated_df.groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

assert null_order_ids == 0, "order_id must not be null"
assert negative_amounts == 0, "amount must be non-negative"
assert duplicate_orders == 0, "duplicate order_id detected"

# -----------------------------------------------------------------------------
# TARGET: Partitioned Parquet in S3 for downstream analytics / warehouse loads
# -----------------------------------------------------------------------------
output_path = "s3://sample-bucket/curated/orders/"

(
    curated_df.write
    .mode("append")
    .format("parquet")
    .partitionBy("event_date")
    .save(output_path)
)

# -----------------------------------------------------------------------------
# WATERMARK: Emit the newest processed timestamp for the next scheduled run
# In production, persist this value in a control table, Parameter Store,
# DynamoDB, or another metadata store after successful completion.
# -----------------------------------------------------------------------------
new_watermark_row = curated_df.agg(F.max("event_ts").alias("new_watermark")).first()
new_watermark = new_watermark_row["new_watermark"] if new_watermark_row else None

print(f"Input watermark: {last_watermark}")
print(f"Output watermark: {new_watermark}")
print(f"Curated records written: {curated_df.count()}")

job.commit()
