# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# Declaring widgets to parameterize the notebook. Parameters helps us to use the entire notebook.
from pyspark.sql.functions import sum, count, when, col, rank, desc
dbutils.widgets.text("parameter_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("parameter_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC Find race years for which the data needs to be processed

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results").filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results").filter(col("race_year").isin(race_year_list))

# COMMAND ----------

driver_standings_df = race_results_df.groupBy("race_year", "driver_name", "driver_nationality") \
                                  .agg(sum("points").alias("total_points"), 
                                       count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# overwrite_partition(final_df, "f1_presentation", "driver_standings", "race_year")

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(final_df, "f1_presentation", "driver_standings", presentation_folder_path, merge_condition, "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_presentation.driver_standings;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_presentation.driver_standings WHERE race_year = 2021;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, count(1) FROM f1_presentation.driver_standings GROUP BY race_year ORDER BY race_year DESC;

# COMMAND ----------


