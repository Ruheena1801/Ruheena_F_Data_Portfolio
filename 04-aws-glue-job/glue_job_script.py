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
# LAST_WATERMARK represents the latest