import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import json

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'data_lake', 'redshift_endpoint', 'redshift_database', 'secret_arn'])


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

secret_name = args['secret_arn']
session = boto3.session.Session()
client = session.client(
    service_name="secretsmanager",
    region_name="us-east-1"
)
try:
    response = client.get_secret_value(SecretId=secret_name)
except ClientError as e:
    raise e
get_secret_value_response = json.loads(response['SecretString'])
redshift_user = get_secret_value_response['username']
redshift_password = get_secret_value_response['password']

df = spark.read.parquet("s3://dbda-grp5-project/Final_Transform_new/Redshift/part-00000-8086bc2d-f453-4c53-9506-1620452f58db-c000.snappy.parquet", header=True)
redshift_url = 'jdbc:redshift://'+args['redshift_endpoint']+':5439/'+args['redshift_database']

df.write \
    .format("io.github.spark_redshift_community.spark.redshift") \
    .option("url", redshift_url) \
    .option("dbtable", "NYC_cft") \
    .option("tempdir", f"s3://{args['data_lake']}/temp-dir/") \
    .option("forward_spark_s3_credentials", True) \
    .option("user", redshift_user) \
    .option("password", redshift_password) \
    .mode("append") \
    .save()

# date = datetime.now().strftime("%Y/%B/%d") # %m for month number, %B for full month name, %b for abbreviated month name

# df1 = spark.read.parquet(f"s3://{args['data_lake']}/rds/{date}", header=True)
# df2 = spark.read.parquet(f"s3://{args['data_lake']}/rds/{date}", header=True)

# df = df1.union(df2)
# df.repartition(1).write.parquet(f"s3://{args['data_lake']}/redshift/{date}")

job.commit()