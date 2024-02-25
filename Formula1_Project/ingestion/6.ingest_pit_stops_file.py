# Databricks notebook source
dbutils.widgets.text("parameter_data_source","")
v_data_source = dbutils.widgets.get("parameter_data_source")

# COMMAND ----------

# Declaring widgets to parameterize the notebook. Parameters helps us to use the entire notebook.
dbutils.widgets.text("parameter_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("parameter_file_date")
print(v_file_date)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1 - Read the JSON File using the spark dataframe Reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp, lit
pitstops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("stop", IntegerType(), False),
    StructField("lap", IntegerType(), False),
    StructField("time", StringType(), False),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pitstops_df = spark.read.schema(pitstops_schema).options(multiLine=True).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - Rename and add new columns

# COMMAND ----------


pitstops_renamed_df = pitstops_df.withColumnsRenamed({"raceId":"race_id","driverId":"driver_id"}) \
                                 .withColumns({"file_date": lit(v_file_date),"data_source":lit(v_data_source)})
pitstops_final_df = add_ingestion_date(pitstops_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the dataframe into parquet file

# COMMAND ----------

# %sql
# DROP TABLE f1_processed.pit_stops;

# COMMAND ----------

# pitstops_final_df.write.mode('overwrite').parquet(f"{processed_folder_path}/pit_stops")
# pitstops_final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.pit_stops")
# overwrite_partition(pitstops_final_df, "f1_processed", "pit_stops", "race_id")
merge_condition = "tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id AND tgt.stop = src.stop"
merge_delta_data(pitstops_final_df, "f1_processed", "pit_stops", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# Read the parquet file
df = spark.read.format("delta").load(f"{processed_folder_path}/pit_stops")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1) FROM f1_processed.pit_stops GROUP BY race_id ORDER BY race_id DESC;

# COMMAND ----------


