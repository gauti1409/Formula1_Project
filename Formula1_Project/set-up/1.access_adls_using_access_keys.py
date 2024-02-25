# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using access keys:
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv
# MAGIC - In order to access the data in a Data lake, we need to set the Spark configuration, "fs.azure.account.key" with the access key of the storage account.
# MAGIC - And then we can use the ABFS(Azure Blob File System) driver to access the data.

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope = "formula1-scope", key="formula1dl-account-key")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formulastorage14dl.dfs.core.windows.net",formula1dl_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formulastorage14dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formulastorage14dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


