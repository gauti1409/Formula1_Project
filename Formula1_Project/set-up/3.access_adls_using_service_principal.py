# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using Service Principal:
# MAGIC 1. Register Azure AD Application/Service Principal.
# MAGIC 2. Generate a secret/password for the application.
# MAGIC 3. Set Spark Config with App/Client Id, Directory/Tenant ID & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor'to the Data Lake. Here what will happen is: What we have done is, we have given the Storage Blob Data Contributor access on our storage account to the Service Principal. So the Service Principal can now access the storage account.

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-client-id")
tenant_id = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-tenant-id")
client_secret = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-client-secret")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formulastorage14dl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formulastorage14dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formulastorage14dl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formulastorage14dl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formulastorage14dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formulastorage14dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formulastorage14dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


