# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("1.ingests_circuits_file", 0, {"parameter_data_source":"Ergast API","parameter_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingests_races_file", 0, {"parameter_data_source":"Ergast API","parameter_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_file", 0, {"parameter_data_source":"Ergast API","parameter_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_file", 0, {"parameter_data_source":"Ergast API","parameter_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results_file", 0, {"parameter_data_source":"Ergast API","parameter_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pit_stops_file", 0, {"parameter_data_source":"Ergast API","parameter_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_times_files", 0, {"parameter_data_source":"Ergast API","parameter_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying_files", 0, {"parameter_data_source":"Ergast API","parameter_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------



# COMMAND ----------


