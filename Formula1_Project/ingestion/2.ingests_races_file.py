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
# MAGIC #####Step 1 - Read the races csv and declare it's schema

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit
races_schema = StructType(fields=[
    StructField("raceId",IntegerType(), False),
    StructField("year",IntegerType(), True),
    StructField("round",IntegerType(), True),
    StructField("circuitId",IntegerType(), True),
    StructField("name",StringType(), True),
    StructField("date",StringType(), True),
    StructField("time",StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read.options(header=True).schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(races_df.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - Concatenating the date and time columns to create race_timestamp and creating ingestion_date as well

# COMMAND ----------

races_modified_df = races_df.withColumns({"race_timestamp" : to_timestamp(concat(col('date'), lit(" "), col('time')),"yyyy-MM-dd HH:mm:ss"),
                                          "data_source": lit(v_data_source), "file_date": lit(v_file_date)})

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 3 -Select only the required columns

# COMMAND ----------

races_selected_df = races_modified_df.drop("URL","date","time")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4- Renaming the columns

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnsRenamed({"raceId":"race_id","circuitId":"circuit_id","year":"race_year"})
races_final_df = add_ingestion_date(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5- Write data to data lake as Parquet file

# COMMAND ----------

# races_final_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/races")
races_final_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# Reading the parquet file to see the data
df = spark.read.format("delta").load(f"{processed_folder_path}/races")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


