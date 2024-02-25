# Databricks notebook source
# MAGIC %md
# MAGIC ##### Read all the data required

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# Declaring widgets to parameterize the notebook. Parameters helps us to use the entire notebook.
dbutils.widgets.text("parameter_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("parameter_file_date")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results").withColumnsRenamed({"time":"race_time", "race_id":"result_race_id",
                                                                                        "file_date": "result_file_date"}) \
                                                                   .filter(f"file_date= '{v_file_date}' ")
constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors").withColumnsRenamed({"name":"team"})
circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits").withColumnsRenamed({"location":"circuit_location"})
races_df = spark.read.format("delta").load(f"{processed_folder_path}/races").withColumnsRenamed({"name":"race_name","race_timestamp":"race_date"})
drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers").withColumnsRenamed({"name":"driver_name","number":"driver_number",    
                                                                                        "nationality":"driver_nationality"})

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join circuits to races

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
                           .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join results to all other dataframes

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id, "inner") \
                             .join(drivers_df, results_df.driver_id == drivers_df.driver_id,"inner") \
                             .join(constructors_df, results_df.constructor_id == constructors_df.constructors_id)

# COMMAND ----------

race_results_selected_df = race_results_df.select("race_id","race_year","race_name","race_date","circuit_location","driver_name","driver_number",
                                                  "driver_nationality","team","grid","fastest_lap","race_time","points","position", "result_file_date") \
                                          .withColumnsRenamed({"result_file_date":"file_date"})
final_df = add_ingestion_date(race_results_selected_df)

# COMMAND ----------

display(final_df.filter("race_year = 2020 and race_name = 'Abu Dhabi Grand Prix'").sort(final_df.points, ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing the data to the presentation layer

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# overwrite_partition(final_df, "f1_presentation", "race_results", "race_id")

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, "f1_presentation", "race_results", presentation_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1) FROM f1_presentation.race_results GROUP BY race_id ORDER BY race_id DESC;

# COMMAND ----------


