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

# driver_standings_df = spark.read.parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

constructors_df = race_results_df.groupBy("race_year","team").agg(sum("points").alias("team_total_points"),
                                                                      count(when(col("position") == 1, True )).alias("team_total_wins"))

# COMMAND ----------

from pyspark.sql.window import Window

constructors_rank_spec = Window.partitionBy("race_year").orderBy(desc("team_total_points"),desc("team_total_wins"))
final_df = constructors_df.withColumn("rank", rank().over(constructors_rank_spec))

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")
# overwrite_partition(final_df, "f1_presentation", "constructor_standings", "race_year")

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(final_df, "f1_presentation", "constructor_standings", presentation_folder_path, merge_condition, "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructor_standings;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructor_standings where race_year=2021;

# COMMAND ----------


