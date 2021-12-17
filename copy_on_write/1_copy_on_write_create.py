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
# Create sample data that we can use to write to S3 as a Hudi dataset
#

# Option #1 ----- Create a Sample Data in this Script ----

data = [
        ("1", "Chris", "2020-01-01", datetime.strptime('2020-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')),
        ("2", "Will", "2020-01-01", datetime.strptime('2020-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')),
        ("3", "Emma", "2020-01-01", datetime.strptime('2020-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')),
        ("4", "John", "2020-01-01", datetime.strptime('2020-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')),
        ("5", "Eric", "2020-01-01", datetime.strptime('2020-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')),
        ("6", "Adam", "2020-01-01", datetime.strptime('2020-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'))
]

schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False), 
        StructField("create_date", StringType(), False),             
        StructField("last_update_time", TimestampType(), False)
])

inputDf = spark.createDataFrame(data=data,schema=schema)

#
# Write data to S3 as a Hudi dataset
# 

# Set hudi options
hudiOptions = {
    'className' : 'org.apache.hudi',
    'hoodie.datasource.hive_sync.use_jdbc':'false',
    'hoodie.datasource.write.precombine.field': 'last_update_time',
    'hoodie.datasource.write.recordkey.field': 'id',
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
    'hoodie.datasource.write.operation': 'insert'
}

combinedConf = {**hudiOptions, **unpartitionDataConfig, **initLoadConfig}

# Write data to S3
glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(inputDf, glueContext, "inputDf"), connection_type = "marketplace.spark", connection_options = combinedConf)
