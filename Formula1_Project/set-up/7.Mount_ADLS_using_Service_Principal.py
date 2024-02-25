# Databricks notebook source
# MAGIC %md
# MAGIC ###Mount Azure Data Lake using Service Principal:
# MAGIC 1. Get client_id, tenant_id and client_secret from Azure Key Vault.
# MAGIC 2. Set Spark Config with App/Client Id, Directory/Tenant ID & Secret
# MAGIC 3. Call file system utility mount to mount the storage.
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-client-id")
tenant_id = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-tenant-id")
client_secret = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-client-secret")

# COMMAND ----------

# spark.conf.set("fs.azure.account.auth.type.formulastorage14dl.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.formulastorage14dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.formulastorage14dl.dfs.core.windows.net", client_id)
# spark.conf.set("fs.azure.account.oauth2.client.secret.formulastorage14dl.dfs.core.windows.net", client_secret)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.formulastorage14dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret":client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formulastorage14dl.dfs.core.windows.net/",
  mount_point = "/mnt/formulastorage14dl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formulastorage14dl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formulastorage14dl/demo/circuits.csv"))

# COMMAND ----------

# To list all the mounts
display(dbutils.fs.mounts())

# COMMAND ----------

# To remove the mounts 
# dbutils.fs.unmount('/mnt/formulastorage14dl/demo')

# COMMAND ----------


