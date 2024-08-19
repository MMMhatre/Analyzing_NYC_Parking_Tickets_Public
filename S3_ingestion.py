import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_Import', 'data_lake'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df_s3 = spark.read.csv(args['S3_Import'],header=True,inferSchema=True)

date = datetime.now().strftime("%Y/%B/%d") # %m for month number, %B for full month name, %b for abbreviated month name
df_s3.repartition(1).write.parquet(f"s3://{args['data_lake']}/s3/{date}")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()
