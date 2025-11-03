import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

jdbc_url = "jdbc:postgresql://rds-endpoint:5432/appdb"
props = {"user":"user","password":"***","driver":"org.postgresql.Driver"}

df = spark.read.jdbc(url=jdbc_url, table="public.orders", properties=props)
df = df.dropDuplicates(["order_id"]).withColumnRenamed("created_at", "event_ts")

glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(df, glueContext, "df"),
    connection_type="s3",
    connection_options={"path": "s3://bucket/bronze/orders/"},
    format="parquet"
)

job.commit()
