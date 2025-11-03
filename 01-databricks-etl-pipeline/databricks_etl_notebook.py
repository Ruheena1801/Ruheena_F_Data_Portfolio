# Databricks PySpark ETL (sample)
from pyspark.sql import functions as F

raw_path = "s3://bucket/raw/sales/"
silver_path = "s3://bucket/silver/sales_curated"

df = (spark.read.format("json").load(raw_path)
      .withColumn("event_date", F.to_date("timestamp"))
      .withColumn("amount", F.col("amount").cast("double"))
      .withColumn("customer_email", F.lower(F.col("customer_email")))
      .dropDuplicates(["order_id"]))

# Basic quality checks
assert df.filter(F.col("order_id").isNull()).count() == 0, "order_id must not be null"

(df.write
 .format("delta")
 .mode("overwrite")
 .partitionBy("event_date")
 .save(silver_path))
