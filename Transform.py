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
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'data_lake', 'redshift_endpoint', 'redshift_database', 'redshift_secret_arn'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

###INGESTION

date = datetime.now().strftime("%Y/%B/%d") # %m for month number, %B for full month name, %b for abbreviated month name

df_rds = spark.read.parquet(f"s3://{args['data_lake']}/rds/{date}")

df_s3 = spark.read.parquet(f"s3://{args['data_lake']}/s3/{date}")

#------------------------------------------------------------------------------------------------------------------------------

###DROPPING COLUMNS

rds_drop = df_rds.drop("Street Code1", "Street Code2", "Street Code3", "Violation Location", "Issuer Precinct", "Issuer Command", "Violation In Front Of Or Opposite", "House Number", "Street Name", "Intersecting Street", "Violation Legal Code", "Days Parking In Effect", "From Hours In Effect", "To Hours In Effect", "Vehicle Color", "Unregistered Vehicle?", "Vehicle Year", "Meter Number", "Feet From Curb", "Violation Post Code", "No Standing or Stopping Violation", "Hydrant Violation", "Double Parking Violation","Issuing Agency","Violation Precinct","Issuer Squad","Date First Observed","Violation Description","Time First Observed","Law Section","Sub Division","Vehicle Expiration Date")

s3_drop = df_s3.drop("Plate","State","License Type","Issue Date","Violation Time","Violation","Judgment Entry Date","Interest Amount","Payment Amount","Precinct","County","Summons Image")

#------------------------------------------------------------------------------------------------------------------------------

### JOINING

df = rds_drop.join(s3_drop,on="Summons Number",how="inner")

#-----------------------------------------------------------------------------------------------------------------------------

### DROPING DUPLICATES AND HANDLING NULLS

df = df.dropDuplicates(["Summons Number"])

df_null = df.na.drop(how="any",thresh=13)

#-----------------------------------------------------------------------------------------------------------------------------

### TRANSFORIMING VIOLATION TIME DATA TYPE TO TIMESTAMP

from pyspark.sql.functions import col, to_timestamp, concat, udf
from pyspark.sql.types import StringType

def changetime(s):
    try:
        if s==None:
            return None
        s = s.strip()
        s = f"{s:0>5}"
        hh, mm, a = int(s[0:2]), int(s[2:4]), s[-1]
        if a == 'A' and hh==12:
            hh=0
        elif a == 'P' and hh!=12:
            hh+=12
        return f"{str(hh):0>2}:{str(mm):0>2}"
    except Exception as e:
        return None

UDF_ChangeTime = udf(changetime, StringType())

df1 = df_null.withColumn("Violation Time 2", UDF_ChangeTime(col("Violation Time")))

df2 = df1.withColumn("Violation_Time", to_timestamp(concat(col("Issue Date"),col("Violation Time 2")), "MM/dd/yyyyHH:mm"))

#-----------------------------------------------------------------------------------------------------------------------------

### TRANSFORIMING STRING DATA TYPE TO DATE

from pyspark.sql.functions import to_date, col

df_date = df2.withColumn("Issue Date",to_date("Issue Date","MM/dd/yyyy"))

df_date = df_date.filter(col("Issue Date").between("2020-01-01", "2023-12-31"))

#------------------------------------------------------------------------------------------------------------------------------

### TRANSFORIMING STRING DATA TYPE TO INTEGER

num_columns = ["Violation Code","Issuer Code"]

df3 = df_date
for col_name in num_columns:
	df3 = df3.withColumn(col_name, col(col_name).cast("int"))

#------------------------------------------------------------------------------------------------------------------------------

### TRANSFORIMING STRING DATA TYPE TO BIGINT

df4 = df3.withColumn("Summons Number",col("Summons Number").cast("bigint"))

#------------------------------------------------------------------------------------------------------------------------------

### TRANSFORIMING STRING DATA TYPE TO DECIMAL

string_columns = ["Fine Amount", "Penalty Amount","Reduction Amount","Amount Due"]

df5 = df4
for col_name in string_columns:
	df5 = df5.withColumn(col_name, col(col_name).cast("decimal(10, 2)"))

#-----------------------------------------------------------------------------------------------------------------------------

### TRANSFORIMING STRING DATA TYPE TO CAPITAL STRING

from pyspark.sql.functions import col, upper

string_columns = ["Vehicle Make", "Violation County","Plate Type","Vehicle Body Type"]

df6 = df5
for col_name in string_columns:
	df6 = df6.withColumn(col_name, upper(col(col_name)))

#----------------------------------------------------------------------------------------------------------------------------

### FILLING NULL VALUES FOR Violation Status

df7 = df6.na.fill("Not Challenged",["Violation Status"])

#-----------------------------------------------------------------------------------------------------------------------------

### CONVERTING ABBREVATIONS AND CODES WITH MEANINGFUL VALUES

state = spark.read.csv("s3://dbda-grp5-project/StateFullForms.csv",header=True,inferSchema=True)
codes = spark.read.csv("s3://dbda-grp5-project/ParkingViolationCodes.csv",header=True,inferSchema=True)
manu = spark.read.csv("s3://dbda-grp5-project/Vehicle_Manu_Converted.csv",header=True,inferSchema=True)
body = spark.read.csv("s3://dbda-grp5-project/Vehicle_Body_Type_converted.csv",header=True,inferSchema=True)
plate = spark.read.csv("s3://dbda-grp5-project/Platetype_Converted.csv",header=True,inferSchema=True)
county = spark.read.csv("s3://dbda-grp5-project/Violation_county_converted.csv",header=True,inferSchema=True)

df8 = df7.join(state,on="Registration State",how="left")
df9 = df8.join(codes,on="Violation Code",how="left")
df10 = df9.join(plate,on="Plate Type",how="left")
df11 = df10.join(manu,on="Vehicle Make",how="left")
df12 = df11.join(county,on="Violation County",how="left")
df13 = df12.join(body,on="Vehicle Body Type",how="left")

df_final = df13.na.fill("Others",["Plate_Type","Vehicle_Body_Type","Vehicle_Make","Violation_County","Issuing Agency"])

final = df_final.drop("Vehicle Body Type","Violation County","Vehicle Make","Plate Type","Violation Code","Registration State","Violation Time","Violation Time 2")

final1 = final.na.drop()

#-------------------------------------------------------------------------------------------------------------------------------------------------------------

### RENAMING COLUMNS FOR REDSHIFT

final2 = final1
for i in final1.columns:
	final2 = final2.withColumnRenamed(i,i.replace(" ","_"))
#--------------------------------------------------------------------------------------------------------------------------

### LOADING DATA IN REDSHIFT

redshift_url = 'jdbc:redshift://'+args['redshift_endpoint']+':5439/'+args['redshift_database']

secret_name = args['redshift_secret_arn']
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

final2.write \
    .format("io.github.spark_redshift_community.spark.redshift") \
    .option("url", redshift_url) \
    .option("dbtable", "NYC_cft") \
    .option("tempdir", f"s3://{args['data_lake']}/temp-dir/") \
    .option("forward_spark_s3_credentials", True) \
    .option("user", redshift_user) \
    .option("password", redshift_password) \
    .mode("append") \
    .save()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()