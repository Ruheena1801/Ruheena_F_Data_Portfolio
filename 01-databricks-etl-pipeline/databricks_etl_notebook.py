# Databricks Medallion ETL Pipeline (portfolio sample)
# Demonstrates Bronze -> Silver -> Gold patterns using PySpark, Delta Lake,
# Auto Loader, checkpoints, data-quality checks, and performance optimization.

from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
raw_path = "s3://sample-bucket/raw/sales/"
bronze_path = "s3://sample-bucket/bronze/sales/"
silver_path = "s3://sample-bucket/silver/sales/"
gold_path = "s3://sample-bucket/gold/daily_sales/"

bronze_checkpoint = "s3://sample-bucket/checkpoints/bronze_sales/"
silver_checkpoint = "s3://sample-bucket/checkpoints/silver_sales/"
schema_location = "s3://sample-bucket/schemas/sales/"

# -----------------------------------------------------------------------------
# BRONZE: Incremental ingestion with Databricks Auto Loader
# -----------------------------------------------------------------------------
bronze_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(raw_path)
    .withColumn("ingestion_ts", F.current_timestamp())
    .withColumn("source_file", F.input_file_name())
)

bronze_query = (
    bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", bronze_checkpoint)
    .outputMode("append")
    .trigger(availableNow=True)
    .start(bronze_path)
)

bronze_query.awaitTermination()

# -----------------------------------------------------------------------------
# SILVER: Clean, standardize, deduplicate, and validate
# -----------------------------------------------------------------------------
silver_df = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
    .withColumn("event_ts", F.to_timestamp("timestamp"))
    .withColumn("event_date", F.to_date("event_ts"))
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("customer_email", F.lower(F.trim(F.col("customer_email"))))
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("amount").isNotNull())
    .withWatermark("event_ts", "1 day")
    .dropDuplicates(["order_id"])
)

silver_query = (
    silver_df.writeStream
    .format("delta")
    .option("checkpointLocation", silver_checkpoint)
    .outputMode("append")
    .trigger(availableNow=True)
    .start(silver_path)
)

silver_query.awaitTermination()

# -----------------------------------------------------------------------------
# DATA QUALITY: Simple reusable checks against the curated Silver layer
# -----------------------------------------------------------------------------
def validate_sales(df):
    assert df.filter(F.col("order_id").isNull()).count() == 0, "order_id must not be null"
    assert df.filter(F.col("event_date").isNull()).count() == 0, "event_date must not be null"
    assert df.filter(F.col("amount") < 0).count() == 0, "amount must be non-negative"
    assert df.groupBy("order_id").count().filter(F.col("count") > 1).count() == 0, "duplicate order_id detected"


silver_batch = spark.read.format("delta").load(silver_path)
validate_sales(silver_batch)

# -----------------------------------------------------------------------------
# GOLD: Business-ready aggregate for analytics
# -----------------------------------------------------------------------------
gold_df = (
    silver_batch
    .groupBy("event_date")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("amount").alias("total_sales"),
        F.avg("amount").alias("avg_order_value")
    )
)

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_path)
)

# -----------------------------------------------------------------------------
# PERFORMANCE: Compact files and improve data skipping for common lookups
# -----------------------------------------------------------------------------
spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (order_id)")

print("Bronze -> Silver -> Gold pipeline completed successfully.")
