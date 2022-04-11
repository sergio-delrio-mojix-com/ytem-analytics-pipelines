# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
import re
import json
import sys
from pyspark.sql import Window as W
from delta import tables as DeltaTables
from pyspark.sql import functions as F, Row

# COMMAND ----------

spark.conf.set("sparkspark.sql.legacy.timeParserPolicy","CORRECTED")

# COMMAND ----------

# ##### Get Parameters for Notebook

# dbutils.widgets.text("params_json","{}")
# params_json = json.loads(dbutils.widgets.get("params_json"))
# expected_topic = params_json["expected_topic"]
# hostname = params_json["kafka_hostname"]

# print(f"Now running POC_Pipeline_SparkStreaming_Consumer for {expected_topic}...")

# COMMAND ----------

expected_topic = "PERN___view___product"
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

# COMMAND ----------

df_expected = df_expected.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)","timestamp").filter("value is not null")

# COMMAND ----------

display(df_expected)

# COMMAND ----------

df_expected.printSchema()

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

# COMMAND ----------

struct_info = [
        "gtin",
        "updateTimeMS",
        "displayGtin",
        "productCode",
        "productLabelShort",
        "productLabelLong",
        "categoryParent",
        "sizeCode",
        "sizeLabel",
        "modelCode",
        "modelLabel",
        "colorCode",
        "colorLabel",
        "seasonCode",
        "seasonLabel",
        "prices",
        "brandCode",
        "brandLabel",
        "vpn",
        "madeinCode",
        "madeinLabel",
        "optionCode",
        "optionLabel",
        "category_l1Code",
        "category_l1Label",
        "category_l2Code",
        "category_l2Label",
        "category_l3Code",
        "category_l3Label",
        "category_l4Code",
        "category_l4Label",
        "expiringTradeItemDaysBeforeExpiration",
        "minimumTradeItemLifespanFromTimeOfArrival",
        "minimumTradeItemLifespanFromTimeOfPacking",
        "minimumTradeItemLifespanFromTimeOfProduction",
      ]

# COMMAND ----------

schema = StructType(
    [
        StructField(x, StringType(), True) for x in struct_info
    ]
)

# COMMAND ----------

df_expected_parsed = df_expected.select(from_json("value", schema).alias("message"),"timestamp")

# COMMAND ----------

df_expected_parsed_values = df_expected_parsed.selectExpr(*['message.'+x for x in struct_info],"timestamp")

# COMMAND ----------

primaryKey = "gtin"
recencyKey = "timestamp"
processing_time = '5 seconds'

# COMMAND ----------

delta_table_path = f"/mnt/poc_discrepancy/{expected_topic}"
delta_table_checkpoint = f"/mnt/poc_discrepancy/{expected_topic}/checkpoint_expected"

# COMMAND ----------

if not DeltaTables.DeltaTable.isDeltaTable(spark, delta_table_path):
  spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)\
      .write\
      .format("delta")\
      .mode("overwrite")\
      .save(delta_table_path)

# COMMAND ----------

# dbutils.fs.rm(delta_table_path,True)
dbutils.fs.rm(delta_table_checkpoint,True)

# COMMAND ----------

def upsertingProducts(df, df_id):
  global primaryKey, recencyKey, bronze_table
  
  # dropping duplicates based on primaryKey, recencyKey 
  w = W.partitionBy(primaryKey)
  df = (
    df.withColumn("max", F.max(F.col(recencyKey))
    .over(w))
    .where(F.col('max') == F.col(recencyKey))
    .drop("max")
  )
  
  (
    bronze_table
   .alias("oldData") 
   .merge(df.alias("newData"), "oldData.{0} = newData.{0}".format(primaryKey))
   .whenMatchedUpdate(set = {x: F.col(f"newData.{x}") for x in [y for y in struct_info if y != primaryKey]})
   .whenNotMatchedInsertAll()
   .execute()
  )
  
  return
  
  
  
  

# COMMAND ----------

bronze_table = DeltaTables.DeltaTable.forPath(spark,delta_table_path)


# COMMAND ----------

query = df_expected_parsed_values.writeStream \
    .queryName(f"query_{expected_topic}_{now}")\
    .trigger(processingTime=processing_time) \
    .option("checkpointLocation", delta_table_checkpoint) \
    .foreachBatch(upsertingProducts) \
    .start()

# COMMAND ----------

display(spark.read.format("delta").load(delta_table_path))

# COMMAND ----------

display(spark.read.format("delta").load(delta_table_path).filter("gtin = '00000026100012'"))
