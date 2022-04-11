# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
import re
import json
import sys

# COMMAND ----------

spark.conf.set("sparkspark.sql.legacy.timeParserPolicy","CORRECTED")

# COMMAND ----------

# ##### Get Parameters for Notebook

# dbutils.widgets.text("params_json","{}")
# params_json = json.loads(dbutils.widgets.get("params_json"))
# tenant = params_json["tenant"]
# expected_topic = params_json["expected_topic"]
# hostname = params_json["kafka_hostname"]

# print(f"Now running POC_Pipeline_SparkStreaming_Consumer for {expected_topic}...")

# COMMAND ----------

expected_topic = "PERN___cache___setting"
hostname = "10.128.0.2:9089"

# COMMAND ----------

# expected_topic = 'PERN___view___product'
# expected_topic = 'PERN___cache___setting'
# expected_topic = 'PERN___view___itemstatus'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Set up an Scheduler Pool to run streams concurrently

# COMMAND ----------

from datetime import datetime as dt
now = dt.now().strftime("%Y%m%d")
pool_name = f"{expected_topic}_pool_{now}"
spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool_name)
pool_name

# COMMAND ----------

df_expected = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", hostname) \
  .option("subscribe", expected_topic) \
  .option("startingOffsets", "earliest") \
  .load()
#   .filter("value is not null")

# COMMAND ----------

df_expected = df_expected\
.selectExpr("CAST(key AS STRING) as settingKey", "CAST(value AS STRING)", "timestamp")\
.filter("value is not null")\
.filter("key = '/defaultConfigName/DispositionCategories/PERN/NotStock'")

# COMMAND ----------

display(df_expected)

# COMMAND ----------

df_expected.printSchema()

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

# COMMAND ----------

struct_info = [
  "feature",
  "configName",
  "pathLocation",
  "key",
  "value",
  "dataType",
  "label",
]

# COMMAND ----------

schema = StructType(
    [
        StructField(x, StringType(), True) for x in struct_info
    ]
)

# COMMAND ----------

df_expected_parsed = df_expected.select("settingKey",from_json("value", schema).alias("message"),"timestamp")

# COMMAND ----------

df_expected_parsed_values = df_expected_parsed.selectExpr("settingKey",*['message.'+x for x in struct_info], "timestamp")

# COMMAND ----------

delta_table_path = f"/mnt/poc_discrepancy/{expected_topic}"
delta_table_checkpoint = f"/mnt/poc_discrepancy/{expected_topic}/checkpoint_expected"

# COMMAND ----------

# dbutils.fs.rm(delta_table_path,True)
dbutils.fs.rm(delta_table_checkpoint,True)

# COMMAND ----------

processing_time = '5 seconds'

# COMMAND ----------

primaryKey = "settingKey"
recencyKey = "timestamp"

# COMMAND ----------

from pyspark.sql import Window as W

# COMMAND ----------

def overwriteDeltaTable(df, df_id):
  global primaryKey, delta_table_path, recencyKey
  w = W.partitionBy(primaryKey)
  df = (
    df.withColumn("max", F.max(F.col(recencyKey))
    .over(w))
    .where(F.col('max') == F.col(recencyKey))
    .drop("max")
  )
  df = df.dropDuplicates(subset=[primaryKey])
  
  if df.count()>0:
    df.write.format("delta").mode("overwrite").save(delta_table_path)
    
  return
  

# COMMAND ----------

query = df_expected_parsed_values.writeStream \
    .queryName(f"query_{expected_topic}_{now}")\
    .trigger(processingTime=processing_time) \
    .option("checkpointLocation", delta_table_checkpoint) \
    .foreachBatch(overwriteDeltaTable) \
    .start()

# COMMAND ----------

display(spark.read.format("delta").load(delta_table_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tmp_view_name_1647966243
# MAGIC AS
# MAGIC SELECT COUNT(1) AS itemsQty , lotNumber AS lotNumber , bizLocation AS bizLocation , disposition AS disposition  
# MAGIC FROM analytics_pern.DM_StockExpirationEPC 
# MAGIC GROUP BY lotNumber , bizLocation , disposition

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmp_view_name_1647966243 limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tmp_view_name_1647966900         AS         SELECT  COUNT(1) AS itemsQty , lotNumber AS lotNumber , bizLocation AS bizLocation , disposition AS disposition  FROM analytics_pern.DM_StockExpirationEPC GROUP BY lotNumber , bizLocation , disposition;

# COMMAND ----------


