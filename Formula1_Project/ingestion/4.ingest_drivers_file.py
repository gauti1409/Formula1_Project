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
# MAGIC ##### Step 1 - Read the JSON File using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import current_timestamp, col, concat, lit
name_schema  = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True) ])
driver_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True) ])

# COMMAND ----------

drivers_df = spark.read.schema(driver_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC #####Step 2 - Rename, add the columns and drop the unwanted columns

# COMMAND ----------


drivers_renamed_df = drivers_df.withColumnsRenamed({"driverId":"driver_id","driverRef":"driver_ref"}) \
                               .withColumns({"name":concat(col("name.forename"), lit(" "), col("name.surname")),
                                             "file_date": lit(v_file_date), "data_source": lit(v_data_source)}) \
                               .drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3 - Add the ingestion date

# COMMAND ----------

drivers_final_df =add_ingestion_date(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4 - Write the dataframe into Parquet file

# COMMAND ----------

# drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# Read the parquet file
df = spark.read.format("delta").load(f"{processed_folder_path}/drivers")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


