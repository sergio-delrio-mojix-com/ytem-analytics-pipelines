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
# expected_topic = params_json["expected_topic"]
# hostname = params_json["kafka_hostname"]
# setting_key = '/defaultConfigName/DispositionCategories/PERN/NotStock'
# print(f"Now running POC_Pipeline_SparkStreaming_Consumer for {expected_topic}...")

# COMMAND ----------

expected_topics = [
   "/mnt/poc_stock_expiration/silver/PERN___view___itemstatus",
   "/mnt/poc_discrepancy/PERN___cache___setting",
   "/mnt/poc_discrepancy/PERN___view___product",
]

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
pool_name = f"POC_SparkStreaming_Consumer_Gold_pool_{now}"
spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool_name)
pool_name

# COMMAND ----------

processing_time = "5 seconds"

# COMMAND ----------

gold_table_path = "/mnt/poc_stock_expiration/gold/PERN___itemstatus"

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime as dt

# COMMAND ----------

def updateGoldTable(df, df_id):
  global expected_topics, df_itemstatus, df_setting, df_product
  df_itemstatus = spark.read.format("delta").option("ignoreChanges","true").load(expected_topics[0])
  df_setting = spark.read.format("delta").option("ignoreChanges","true").load(expected_topics[1])
  df_product = spark.read.format("delta").option("ignoreChanges","true").load(expected_topics[2])
  df_gold = (
    df_itemstatus.filter("product_gtin is not null").crossJoin(
      df_setting.select(F.col("value").alias("setting_value"))
    )
    .filter(~F.col("disposition").isin(F.col('setting_value')))
    .join(
      df_product.withColumnRenamed("gtin","product_gtin"),
      on="product_gtin",
      how="left"
    )
    .withColumn("execution_date",F.lit(dt.now().strftime("%Y%m%d %H%M%S")))
  )
  df_gold.write.format("delta").mode("overwrite").save(gold_table_path)
  return
  
  
  
  

# COMMAND ----------

def triggerSequentialOverwrite(df, df_id):
  global q
  q.put("overwrite triggered.")
  return

# COMMAND ----------

def stream_initialization(expected_topic):
  global processing_time, updateGoldTable
  return spark\
  .readStream\
  .format("delta")\
  .option("ignoreChanges","true")\
  .load(expected_topic)\
  .writeStream\
  .queryName(f"query_{expected_topic}_{now}")\
  .trigger(processingTime=processing_time)\
  .option("checkpointLocation", "{}/checkpoint".format(expected_topic))\
  .foreachBatch(triggerSequentialOverwrite)\
  .start()

# COMMAND ----------

dbutils.fs.rm(gold_table_path,True)
[dbutils.fs.rm("{}/checkpoint".format(expected_topic),True) for expected_topic in expected_topics]

# COMMAND ----------

from pyspark import InheritableThread
from queue import Queue

# COMMAND ----------

def sequential_worker(q):
  print("Starting worker thread.")
  while True:
    value = q.get()
    if value == "QUIT":
      break
    else:
      updateGoldTable(-1,-1)
      print("table overwritten!!!")
  print("Exiting thread.")
  return

# COMMAND ----------

q = Queue()
t = InheritableThread(target=sequential_worker,args=(q,))
t.start()

# COMMAND ----------

# MAGIC %md ### ItemStatus silver consumer

# COMMAND ----------

stream_initialization(expected_topics[0])

# COMMAND ----------

# MAGIC %md ### Settings silver consumer

# COMMAND ----------

stream_initialization(expected_topics[1])

# COMMAND ----------

# MAGIC %md ### Product silver consumer

# COMMAND ----------

stream_initialization(expected_topics[2])

# COMMAND ----------

# MAGIC %md ## Items in stock for 9999999

# COMMAND ----------

display(spark.read.format("delta").load(gold_table_path)
       .filter("site_code = '9999999'"))

# COMMAND ----------

(spark.read.format("delta").load(gold_table_path)
       .filter("site_code = '9999999'").count())

# COMMAND ----------

# MAGIC %md ## Hexa per Category_l1Label for 9999999

# COMMAND ----------

display(spark.read.format("delta").load(gold_table_path)
        .filter("site_code = '9999999'")
        .groupby("category_l1Label")
        .agg(F.count("hexa"))
       )

# COMMAND ----------

# display(spark.read.format("delta").load(gold_table_path).groupby("category_l1Label").agg(F.count("hexa")))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE analytics_pern.DM_StockExpirationEPC
# MAGIC   USING DELTA
# MAGIC   LOCATION '/mnt/poc_stock_expiration/gold/PERN___itemstatus'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE analytics_pern
