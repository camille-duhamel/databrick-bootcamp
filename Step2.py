# Databricks notebook source
# MAGIC %fs ls /mnt/data_raw

# COMMAND ----------

df_GH_dist = spark.read.option("header","true").option("inferSchema", "true").option("sep", ";").csv("/mnt/data_raw/GreatHouse Holdings District.csv")
df_real_estate = spark.read.option("header","true").option("inferSchema", "true").option("sep", ",").csv("/mnt/data_raw/Real Estate Ad.csv")
df_RB_dist = spark.read.option("header","true").option("inferSchema", "true").option("sep", ";").csv("/mnt/data_raw/Roger&Brothers District.csv")
df_GH_stock = spark.read.option("header","true").option("inferSchema", "true").option("sep", ";").csv("/mnt/data_raw/Stock GreatHouse Holdings.csv")
df_RB_stock = spark.read.option("header","true").option("inferSchema", "true").option("sep", ";").csv("/mnt/data_raw/Stock Roger&Brothers.csv")

# COMMAND ----------

df_GH_dist.printSchema()
df_GH_stock.printSchema()
df_RB_dist.printSchema()
df_RB_stock.printSchema()
df_real_estate.printSchema()

# COMMAND ----------

display(df_GH_dist)
display(df_GH_stock)
display(df_RB_dist)
display(df_RB_stock)
display(df_real_estate)

# COMMAND ----------

print(df_GH_stock.count())
print(df_RB_stock.count())

# COMMAND ----------

print(df_GH_dist.count())
print(df_RB_dist.count())

# COMMAND ----------

print(df_real_estate.count())

# COMMAND ----------

df_stock_all = df_GH_stock.union(df_RB_stock)
print(df_stock_all.count())

# COMMAND ----------

df_stock_all = df_stock_all.drop_duplicates()
print(df_stock_all.count())

# COMMAND ----------

display(df_stock_all)

# COMMAND ----------

# Change the price type String into double 

from pyspark.sql.functions import *
df_stock_all = df_stock_all.withColumn('price', regexp_replace('price', ',', '.'))
df_stock_all = df_stock_all.withColumn("price",col("price").cast("double"))

# COMMAND ----------

# Statistic
df_stock_all.summary("count", "min", "1%", "5%", "10%", "25%", "75%", "90%", "95%", "99%", "max").show()

# COMMAND ----------

df_temp = df_stock_all.summary("count", "min", "1%", "5%", "10%", "25%", "75%", "90%", "95%", "99%", "max")

# COMMAND ----------

df_temp.printSchema()

# COMMAND ----------

df_temp.show()

# COMMAND ----------

df_dist_all = df_GH_dist.union(df_RB_dist)
print(df_dist_all.count())

df_dist_all = df_dist_all.drop_duplicates()
print(df_dist_all.count())

display(df_dist_all)

# COMMAND ----------

# join_cond = [df_dist_all.longitude == df_real_estate.longitude, df_dist_all.latitude == df_real_estate.latitude]
# df_join = df_dist_all.join(df_real_estate, join_cond, "inner")

# COMMAND ----------

import pandas as pd
df_dist_all = df_dist_all.toPandas()
df_real_estate = df_real_estate.toPandas()

# COMMAND ----------

df_merge = pd.merge(df_dist_all, df_real_estate, how='left', left_on=['longitude','latitude'], right_on = ['longitude','latitude'])

# COMMAND ----------

print(df_merge.count())
display(df_merge)

# COMMAND ----------

# MAGIC %md
# MAGIC Export data into silver folder

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://silver@databrickbootcampstorage.blob.core.windows.net",
  mount_point = "/mnt/data_temp",
  extra_configs = {"fs.azure.account.key.databrickbootcampstorage.blob.core.windows.net":dbutils.secrets.get(scope = "scopeName", key = "key")})

# COMMAND ----------

df_stock_all.write.mode("overwrite").option("header", "true").csv("/mnt/data_temp/Stock All")

# COMMAND ----------

df_merge = spark.createDataFrame(df_merge)

# COMMAND ----------

df_merge.write.mode("overwrite").option("header", "true").csv("/mnt/data_temp/Real Estate Ad")
