# Databricks notebook source
dbutils.widgets.text("parameter_data_source","")
v_data_source = dbutils.widgets.get("parameter_data_source")

# COMMAND ----------

# Declaring widgets to parameterize the notebook. Parameters helps us to use the entire notebook.
dbutils.widgets.text("parameter_file_date","2021-04-18")
v_file_date = dbutils.widgets.get("parameter_file_date")
print(v_file_date)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1 - Read the CSV Files using the spark dataframe Reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp, lit
lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("lap", IntegerType(), False),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - Rename and add new columns

# COMMAND ----------


lap_times_renamed_df = lap_times_df.withColumnsRenamed({"raceId":"race_id","driverId":"driver_id"}) \
                                 .withColumns({"file_date": lit(v_file_date), "data_source":lit(v_data_source)})
lap_times_final_df = add_ingestion_date(lap_times_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the dataframe into parquet file

# COMMAND ----------

# %sql
# DROP TABLE f1_processed.lap_times;

# COMMAND ----------

# lap_times_final_df.write.mode('overwrite').parquet(f"{processed_folder_path}/lap_times")
# lap_times_final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.lap_times")
# overwrite_partition(lap_times_final_df, 'f1_processed', 'lap_times', 'race_id')
merge_condition = "tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id AND tgt.lap = src.lap"
merge_delta_data(lap_times_final_df, "f1_processed", "lap_times", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# Read the parquet file
df = spark.read.format("delta").load(f"{processed_folder_path}/lap_times")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.lap_times;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1) FROM f1_processed.lap_times GROUP BY race_id ORDER BY race_id DESC;

# COMMAND ----------


