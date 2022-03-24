# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %fs ls /mnt/data_temp

# COMMAND ----------

destFile_stock = "/mnt/data_temp/Stock All"
destFile_ad = "/mnt/data_temp/Real Estate Ad"

df_stock_all = spark.read.option("header", "true").option("inferschema","true").csv(destFile_stock)
df_real_est_ad = spark.read.option("header", "true").option("inferschema","true").csv(destFile_ad)

# COMMAND ----------

display(df_stock_all)
display(df_real_est_ad)

# COMMAND ----------

df_stock_all.printSchema()

# COMMAND ----------

display(df_stock_all)

# COMMAND ----------

df_temp = df_stock_all.summary("count", "min", "1%", "5%", "10%", "25%", "75%", "90%", "95%", "99%", "max")
df_temp.show()

# COMMAND ----------

val_room_99 = float(df_temp.filter(col("summary") == "99%").select("Rooms").collect()[0][0])

# COMMAND ----------

# Filtre sur les Rooms, on ne garde que les observations où Rooms <= 12
df_stock_all = df_stock_all.filter(df_stock_all.Rooms<= val_room_99 )

# COMMAND ----------

df_stock_all = df_stock_all.withColumn('price', regexp_replace('price', ',', '.'))
df_stock_all = df_stock_all.withColumn("price",col("price").cast("double"))

# COMMAND ----------

# Suppression des 1% des observations les plus basses 
df_stock_all = df_stock_all.filter(df_stock_all.price > float(df_temp.where(col("summary") == "1%").select("price").collect()[0][0]))

# COMMAND ----------

# MAGIC %md
# MAGIC Analyse the average price per number of rooms

# COMMAND ----------

df_priceRooms = df_stock_all.select("Rooms", "price").groupBy("Rooms").agg(avg("price").alias("mean")).orderBy("Rooms")
df_priceRooms.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Analyse the different indicators, the lowest, highest, average of price of each district

# COMMAND ----------

display(df_stock_all)
print(df_stock_all.count())

# COMMAND ----------

# Concat longitude and latitude columns to create a district ID 
df_stock_all = df_stock_all.withColumn("district",concat(col("longitude"),lit(","), col("latitude")))

# COMMAND ----------

df_stat_dist = df_stock_all.select("district", "price").groupBy("district").agg(min("price").alias("min"), max("price").alias("max"), avg("price").alias("mean"))

# COMMAND ----------

print(df_stat_dist.count())
df_stat_dist.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Which kind of house is the most in the market (Such as how many rooms, how many bedrooms)

# COMMAND ----------

# House the most in the market with rooms option
df_most_rooms = df_stock_all.groupBy("Rooms").count().orderBy("count", ascending=False)
df_most_rooms.show()

# COMMAND ----------

# House the mosr in the market with bedrooms option
df_most_bedrooms = df_stock_all.groupBy("Bedrooms").count().orderBy("count", ascending=False)
df_most_bedrooms.show()

# COMMAND ----------

# MAGIC %md
# MAGIC For different price range, analyse the difference of houses (Nb of rooms, bedrooms) 

# COMMAND ----------

df_temp = df_stock_all.summary("count", "min", "25%", "50%", "75%", "max")

# COMMAND ----------

df_temp.show()

# COMMAND ----------

val_25 = float(df_temp.filter(col("summary") == "25%").select("price").collect()[0][0])
val_50 = float(df_temp.filter(col("summary") == "50%").select("price").collect()[0][0])
val_75 = float(df_temp.filter(col("summary") == "75%").select("price").collect()[0][0]) 

# COMMAND ----------

del df_temp

# COMMAND ----------

# Statistic for 25% house with the lower price 
df_house_25_summary = df_stock_all.select("Rooms", "Bedrooms", "price").filter(df_stock_all.price < val_25).summary()
df_house_25_summary.show()

# COMMAND ----------

# Statistic for house between 25% and 50% price 

df_house_25_50_summary = df_stock_all.select("Rooms", "Bedrooms", "price").filter((df_stock_all.price > val_25 ) & (df_stock_all.price < val_50)).summary()
df_house_25_50_summary.show()

# COMMAND ----------

# Statistic for house between 50% and 75% price 

df_house_50_75_summary = df_stock_all.select("Rooms", "Bedrooms", "price").filter((df_stock_all.price > val_50) & (df_stock_all.price < val_75)).summary()
df_house_50_75_summary.show()

# COMMAND ----------

# Statistic for 75% house with the higher price 
df_house_75_summary = df_stock_all.select("Rooms", "Bedrooms", "price").filter(df_stock_all.price > val_75).summary()
df_house_75_summary.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Export data into gold folder

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://gold@databrickbootcampstorage.blob.core.windows.net",
  mount_point = "/mnt/data_final",
  extra_configs = {"fs.azure.account.key.databrickbootcampstorage.blob.core.windows.net":dbutils.secrets.get(scope = "scopeName", key = "key")})

# COMMAND ----------

df_priceRooms

# COMMAND ----------

df_stock_all.write.mode("overwrite").option("header", "true").csv("/mnt/data_final/Stock All Cleaned")
df_priceRooms.write.mode("overwrite").option("header", "true").csv("/mnt/data_final/Avg Price Room")
df_stat_dist.write.mode("overwrite").option("header", "true").csv("/mnt/data_final/Stat District")
df_most_rooms.write.mode("overwrite").option("header", "true").csv("/mnt/data_final/Rooms on Market")
df_most_bedrooms.write.mode("overwrite").option("header", "true").csv("/mnt/data_final/Bedrooms on Market")
df_house_25_summary.write.mode("overwrite").option("header", "true").csv("/mnt/data_final/Stat House 25")
df_house_25_50_summary.write.mode("overwrite").option("header", "true").csv("/mnt/data_final/Stat House 25 50")
df_house_50_75_summary.write.mode("overwrite").option("header", "true").csv("/mnt/data_final/Stat House 50 75")
df_house_75_summary.coalesce(1).write.mode("overwrite").option("header", "true").csv("/mnt/data_final/Stat House 75")
