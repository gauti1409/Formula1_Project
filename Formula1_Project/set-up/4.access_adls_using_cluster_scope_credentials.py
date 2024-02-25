# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using Cluster Scope Credentials:
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formulastorage14dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formulastorage14dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


