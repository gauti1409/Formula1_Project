# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

races_filter_df_sql = races_df.filter("race_year = 2019 and round <= 5")

# COMMAND ----------

races_filter_df_py = races_df.filter((races_df.race_year == 2019) & (races_df.round <= 5))

# COMMAND ----------

display(races_filter_df_sql)

# COMMAND ----------

display(races_filter_df_py)

# COMMAND ----------


