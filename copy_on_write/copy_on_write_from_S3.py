import sys
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import *
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer').config('spark.sql.hive.convertMetastoreParquet','false').getOrCreate()
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#
# Read the example data
#

# ----- Read from Glue Catalog ----

input_Dynamic_Frame = glueContext.create_dynamic_frame.from_catalog(database = "hudi-glue", table_name = "yellow_cab_input_data", transformation_ctx = "transformation_0")
inputDf = input_Dynamic_Frame.toDF()
inputDf = inputDf.withColumn("pk_col",monotonically_increasing_id() + 1)

# ----- Read directly from S3 ----

# Schema for NYC Taxi Data
yt_schema = StructType([
    StructField("vendorid",IntegerType(),True),
    StructField("tpep_pickup_datetime",TimestampType(),True),
    StructField("tpep_dropoff_datetime",TimestampType(),True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("ratecodeid", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("pulocationid", IntegerType(), True),
    StructField("dolocationid", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("pk_col", LongType(), True)
])

# Read the data
# inputDf = spark.read.schema(yt_schema).option("header", "true").csv("s3://hudi-chrisshark-glue/yellow_cab_input_data/yellow_tripdata_2020-01.csv").withColumn("pk_col",monotonically_increasing_id() + 1)

# Set hudi options
hudiOptions = {
    'className' : 'org.apache.hudi',
    'hoodie.datasource.hive_sync.use_jdbc':'false',
    'hoodie.datasource.write.precombine.field': 'tpep_pickup_datetime',
    'hoodie.datasource.write.recordkey.field': 'pk_col',
    'hoodie.table.name': 'copy_on_write_glue',
    'hoodie.consistency.check.enabled': 'true',
    'hoodie.datasource.hive_sync.database': 'default',
    'hoodie.datasource.hive_sync.table': 'copy_on_write_glue',
    'hoodie.datasource.hive_sync.enable': 'true',
    'path': 's3://hudi-chrisshark-glue/copy_on_write_glue'
}

unpartitionDataConfig = {
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor',
    'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'
}

initLoadConfig = {
    'hoodie.bulkinsert.shuffle.parallelism': 68,
    'hoodie.datasource.write.operation': 'bulk_insert'
}

combinedConf = {**hudiOptions, **unpartitionDataConfig, **initLoadConfig}

# Write data to S3
glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(inputDf, glueContext, "inputDf"), connection_type = "marketplace.spark", connection_options = combinedConf)
