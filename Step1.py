# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://bronze@databrickbootcampstorage.blob.core.windows.net",
  mount_point = "/mnt/data_raw",
  extra_configs = {"fs.azure.account.key.databrickbootcampstorage.blob.core.windows.net":dbutils.secrets.get(scope = "scopeName", key = "key")})

# COMMAND ----------

dbutils.fs.ls("/mnt/data_raw/")

# COMMAND ----------

# MAGIC %fs ls /mnt/data_raw
