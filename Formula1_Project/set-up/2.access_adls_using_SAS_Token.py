# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using SAS Token:
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv
# MAGIC - The first parameter here defines the authentication type as SAS or Shared Access Signature.
# MAGIC - The sceond one defines the SAS token provider to fixed SAS token provider.
# MAGIC - The last one is the one in which we will set the value of the SAS Token.

# COMMAND ----------

formula1_demo_sas_token = dbutils.secrets.get(scope = "formula1-scope", key="formula1-demo-SAS-Token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formulastorage14dl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formulastorage14dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formulastorage14dl.dfs.core.windows.net", formula1_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formulastorage14dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formulastorage14dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


