# Databricks notebook source
# MAGIC %md
# MAGIC Data Viusualization

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

# Read data from gold folder

df_stock_all = spark.read.option("header", "true").option("inferschema","true").csv("/mnt/data_final/Stock All Cleaned")
df_priceRooms = spark.read.option("header", "true").option("inferschema","true").csv("/mnt/data_final/Avg Price Room")
df_stat_dist = spark.read.option("header", "true").option("inferschema","true").csv("/mnt/data_final/Stat District")
df_most_rooms = spark.read.option("header", "true").option("inferschema","true").csv("/mnt/data_final/Rooms on Market")
df_most_bedrooms = spark.read.option("header", "true").option("inferschema","true").csv("/mnt/data_final/Bedrooms on Market")
df_house_25_summary = spark.read.option("header", "true").option("inferschema","true").csv("/mnt/data_final/Stat House 25")
df_house_25_50_summary = spark.read.option("header", "true").option("inferschema","true").csv("/mnt/data_final/Stat House 25 50")
df_house_50_75_summary = spark.read.option("header", "true").option("inferschema","true").csv("/mnt/data_final/Stat House 50 75")
df_house_75_summary = spark.read.option("header", "true").option("inferschema","true").csv("/mnt/data_final/Stat House 75")

# COMMAND ----------

df_priceRooms.toPandas().plot(kind="bar", x="Rooms", y="mean", label="moyenne", alpha=0.4, figsize=(10,7))
plt.title("Mean by number of rooms")
plt.show()

# COMMAND ----------

# DBTITLE 1,Average price per number of rooms
# Average price per number of rooms
display(df_priceRooms)

# COMMAND ----------

# DBTITLE 1,Average price per number of rooms
display(df_stock_all)

# COMMAND ----------

display(df_stock_all)

# COMMAND ----------

# DBTITLE 1,Market description with number of rooms
display(df_most_rooms)

# COMMAND ----------

df_most_bedrooms.orderBy("Bedrooms").toPandas().plot(kind="bar", x="Bedrooms", y="count", label="Nombre de Chambre")
plt.title("Sum of house with Bedrooms number")
plt.show()

# COMMAND ----------

df_most_rooms.orderBy("Rooms").toPandas().plot(kind="bar", x="Rooms", y="count", label="Nombre de pièces")
plt.title("Sum of house with Bedrooms number")
plt.show()

# COMMAND ----------

# DBTITLE 1,Box-Plot for price data 
display(df_stock_all)

# COMMAND ----------


