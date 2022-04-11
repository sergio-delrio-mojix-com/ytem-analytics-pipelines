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
spark.conf.set("spark.databricks.delta.formatCheck.enabled","false")

# COMMAND ----------

# ##### Get Parameters for Notebook

# dbutils.widgets.text("params_json","{}")
# params_json = json.loads(dbutils.widgets.get("params_json"))
# expected_topic = params_json["expected_topic"]
# hostname = params_json["kafka_hostname"]
# setting_key = '/defaultConfigName/DispositionCategories/PERN/NotStock'
# print(f"Now running POC_Pipeline_SparkStreaming_Consumer for {expected_topic}...")

# COMMAND ----------

expected_topic = "PERN___view___itemstatus"
consumer_group = "PERN-databricks-itemstatus-consumer"
hostnames = ["10.128.0.25:9092",
            "10.128.0.24:9092",
            "10.128.0.23:9092"]
setting_key = '/defaultConfigName/DispositionCategories/PERN/NotStock'

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
  .option("kafka.bootstrap.servers", ",".join(hostnames)) \
  .option("subscribe", expected_topic) \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .option("group_id",consumer_group) \
  .load()
#   .filter("value is not null")

# COMMAND ----------

df_expected = df_expected.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)","timestamp").filter("value is not null")

# COMMAND ----------

df_expected.printSchema()

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

# COMMAND ----------

struct_info = [
  "_id",
  "bizLocation",
  "cycleCountDate",
  "site_code",
  "disposition",
  "epc",
  "eventTime",
  "firstSeenTime",
  "product_gtin",
  "hexa",
  "lotNumber",
  "itemExpirationDate",
  "bestBeforeDate",
  "harvestStartDate",
  "harvestEndDate",
  "sellByDate",
  "recordTime",
  "viz_action",
]

# COMMAND ----------

schema = StructType(
    [
        StructField(x, StringType(), True) for x in struct_info
    ]
)

# COMMAND ----------

df_expected_parsed = df_expected.select(from_json("value", schema).alias("message"),"timestamp").withColumn("settingKey",F.lit(setting_key))

# COMMAND ----------

df_expected_parsed_values = df_expected_parsed.selectExpr(*['message.'+x for x in struct_info],"timestamp")

# COMMAND ----------

delta_table_path = f"/mnt/poc_discrepancy/{expected_topic}"
delta_table_checkpoint = f"/mnt/poc_discrepancy/{expected_topic}/checkpoint_expected"

# COMMAND ----------


# dbutils.fs.rm(delta_table_path,True)
dbutils.fs.rm(delta_table_checkpoint,True)

# COMMAND ----------

# MAGIC %md ### 0.-Saving historic ItemStatus records

# COMMAND ----------

query = df_expected_parsed_values.writeStream \
    .format("delta") \
    .option("path", delta_table_path) \
    .option("checkpointLocation", delta_table_checkpoint) \
    .outputMode("append") \
    .start()

# COMMAND ----------

# MAGIC %md ## 1.- Reaggregating recieved to retain last status for each ItemStatus

# COMMAND ----------

from delta import tables as DeltaTables
from pyspark.sql import functions as F, Row

# COMMAND ----------

primaryKey = "hexa"

# COMMAND ----------

recencyKey = "eventTime"

# COMMAND ----------

from pyspark.sql import Window as W

# COMMAND ----------

def itemStatus_upsert(microBatchDf, BatchId):
  global silver_delta, silver_delta_df, primaryKey, struct_info, recencyKey
  
  # dropping batch duplicates by recencyKey
  w = W.partitionBy(primaryKey)
  unique_rows_df = (
    microBatchDf.withColumn("max", F.max(F.col(recencyKey))
    .over(w))
    .where(F.col('max') == F.col(recencyKey))
    .drop("max")
  )
  
  # dropping rows older than current itemStatus recencyKey
  recent_rows_df = None
  if silver_delta_df.count() > 0:
    recent_rows_df = (
      unique_rows_df.alias("newData").join(
        silver_delta_df.select(F.col(primaryKey).alias(f"{primaryKey}_y"),
                               F.col(recencyKey).alias(f"{recencyKey}_y")).alias("oldData"),
        F.expr("(newData.{0} = oldData.{0}_y)  and (newData.{1} < oldData.{1}_y)".format(primaryKey,recencyKey)),
        "left_anti"
      ).drop(*[f"{primaryKey}_y",f"{recencyKey}_y"])
   )
  else:
    recent_rows_df = unique_rows_df
  
  # upserting reminiscent rows
  (
    silver_delta
   .alias("oldData") 
   .merge(recent_rows_df.alias("newData"), "oldData.{0} = newData.{0}".format(primaryKey))
   .whenMatchedUpdate(set = {x: F.col(f"newData.{x}") for x in [y for y in struct_info if y != primaryKey]})
   .whenNotMatchedInsertAll()
#    .whenNotMatchedInsert(values = {x: F.col(f"newData.{x}") for x in struct_info})
   .execute()
  )
  
  return

# COMMAND ----------

silver_delta_table_path = f"/mnt/poc_stock_expiration/silver/{expected_topic}"
silver_delta_table_checkpoint = f"/mnt/poc_stock_expiration/silver/{expected_topic}/checkpoint_expected"

# COMMAND ----------

dbutils.fs.rm(silver_delta_table_checkpoint,True)

# COMMAND ----------

if not DeltaTables.DeltaTable.isDeltaTable(spark, silver_delta_table_path):
  spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)\
      .write\
      .format("delta")\
      .mode("overwrite")\
      .save(silver_delta_table_path)

# COMMAND ----------

silver_delta = DeltaTables.DeltaTable.forPath(spark,silver_delta_table_path)

# COMMAND ----------

silver_delta_df = spark.read.format("delta").load(silver_delta_table_path)

# COMMAND ----------

processing_time = "1 seconds"

# COMMAND ----------

df_expected_parsed_values.writeStream\
     .queryName(f"query_{expected_topic}_{now}")\
     .trigger(processingTime=processing_time)\
     .option("checkpointLocation", silver_delta_table_checkpoint)\
     .foreachBatch(itemStatus_upsert)\
     .start()
