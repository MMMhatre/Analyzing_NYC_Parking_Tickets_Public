import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
import boto3
import json
from botocore.exceptions import ClientError

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RDS_endpoint', 'RDS_secret_arn', 'data_lake'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
url= args['RDS_endpoint']

secret_name = args['RDS_secret_arn']
session = boto3.session.Session()
client = session.client(service_name="secretsmanager", region_name="us-east-1")
try:
    response = client.get_secret_value(SecretId=secret_name)
except ClientError as e:
    raise e
get_secret_value_response = json.loads(response['SecretString'])
rds_user = get_secret_value_response['username']
rds_password = get_secret_value_response['password']

prop = {
    "user": rds_user,
    "password": rds_password
}

df1 = spark.read.jdbc(url=url,table="data_2021",properties=prop)
df2 = spark.read.jdbc(url=url,table="data_2022",properties=prop)
df3 = spark.read.jdbc(url=url,table="data_2023",properties=prop)
df = df1.union(df2).union(df3)

date = datetime.now().strftime("%Y/%B/%d") # %m for month number, %B for full month name, %b for abbreviated month name
df.repartition(1).write.parquet(f"s3://{args['data_lake']}/rds/{date}")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()