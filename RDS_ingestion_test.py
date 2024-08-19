import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RDS', 'data_lake'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

date = datetime.now().strftime("%Y/%B/%d") # %m for month number, %B for full month name, %b for abbreviated month name

df = spark.read.csv("s3://bucket-made-using-cli/data/iris.csv", header=True)
df.repartition(1).write.parquet(f"s3://{args['data_lake']}/rds/{date}")

job.commit()