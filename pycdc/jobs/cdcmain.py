from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField
import json
from pycdc.jobs import cdcType2

import os
#import sys
#from pycdc.utilities import log_utilities

#import logging

'''
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
#handler = logging.FileHandler('/spark/jobs/driver.log')
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s -'  ' %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

'''



'''from  pycdc.utilities import logger
logger = logger.CustomAppLogger()'''
from pycdc.utilities import app_logger
logger = app_logger.setUpLogging(__name__)

logger.info("Spark Session for application CDC is about to create")
spark = SparkSession.builder.master("local[*]").appName("CDC").getOrCreate()

logger.info("Spark Session created...")

logger.info("Intializing the Spark Logger")
sc = spark.sparkContext
log4jLogger = sc._jvm.org.apache.log4j
logger_pyspark = log4jLogger.LogManager.getLogger(__name__)
logger_pyspark.info("pyspark script logger initialized")

#Reading the configuration files
with open("C:\\Users\\Vishnu59\\PycharmProjects\\Pyspark_cdcframework\\pycdc\\configs\\cdc-application.json","r") as f:
    conf = json.load(f)

start_dt=conf.get("start_dt")
end_dt=conf.get("end_dt")
end_value=conf.get("end_value")
unique_columns=conf.get("unique_columns")
end_dt_logic=conf.get("end_dt_logic")
format=conf.get("format")
hist_path = conf.get("history_path")
snap_path = conf.get("snapshot_path")
opt_path = conf.get("output_path")
schema_loc_hist = conf.get("schema_loc_hist")
schema_loc_snap = conf.get("schema_loc_snap")

logger.info("Read the configuration file ...")

print(end_value)


logger.info("Read the Schema  Hist")
with open(schema_loc_hist) as f:
    hist_schema = StructType.fromJson(json.load(f))
    logger.info("Read the Schema  Hist is '%s'",hist_schema )

logger.info("Read the Schema  Snap")
with open(schema_loc_snap) as f:
    snap_schema = StructType.fromJson(json.load(f))
    logger.info("Read the Schema Snapshot is '%s'", snap_schema)


#hist = spark.read.option("delimiter","|").option("header",True).option("inferSchema",True).format(format).load(hist_path)
hist = spark.read.schema(hist_schema).option("delimiter","|").format(format).load(hist_path)
logger.info("History file read ...")
#snap = spark.read.option("delimiter","|").option("header",True).option("inferSchema",True).format(format).load(snap_path)
snap = spark.read.schema(snap_schema).option("delimiter","|").format(format).load(snap_path)
logger.info("Snapshot file read ...")


print("INPUT DF1 :" )
hist.show()
print(hist.schema)
logger.info("Schema of Hist file is '%s'",hist.schema)
print("INPUT DF2 :")
snap.show()

#print(str([start_dt,end_value].extend(unique_columns.split(","))))
#print(str([start_dt,end_value] + unique_columns.split(",")))
cdcType2.cdcType2Main(spark,hist,snap,start_dt,end_dt,end_value,unique_columns,end_dt_logic)