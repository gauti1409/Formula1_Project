# Databricks notebook source
# Declaring widgets to parameterize the notebook. Parameters helps us to use the entire notebook.
dbutils.widgets.text("parameter_data_source","")
v_data_source = dbutils.widgets.get("parameter_data_source")

# COMMAND ----------

# Declaring widgets to parameterize the notebook. Parameters helps us to use the entire notebook.
dbutils.widgets.text("parameter_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("parameter_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3 - Rename columns and add ingestion date

# COMMAND ----------


constructor_renamed_df = constructor_dropped_df.withColumnsRenamed({"constructorId":"constructors_id","constructorRef":"constructor_ref"}) \
                                            .withColumns({"file_date": lit(v_file_date), "data_source":lit(v_data_source)})
constructor_final_df = add_ingestion_date(constructor_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4 - Write dataframe to Parquet file

# COMMAND ----------

# constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# Read the parquet file
df = spark.read.format("delta").load(f"{processed_folder_path}/constructors")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")
