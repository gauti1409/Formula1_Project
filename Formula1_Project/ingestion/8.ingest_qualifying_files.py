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
# MAGIC #####Step 1 - Read Multiple JSON Files using the spark dataframe Reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp, lit
qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("constructorId", IntegerType(), False),
    StructField("number", IntegerType(), False),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).options(multiLine=True).json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - Rename and add new columns

# COMMAND ----------


qualifying_renamed_df = qualifying_df.withColumnsRenamed({"raceId":"race_id","driverId":"driver_id",
                                                        "qualifyId":"qualifying_id", "constructorId":"constructor_id"}) \
                                 .withColumns({"file_date": lit(v_file_date),"data_source": lit(v_data_source)})
qualifying_final_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the dataframe into parquet file

# COMMAND ----------

# qualifying_final_df.write.mode('overwrite').parquet(f"{processed_folder_path}/qualifying")
# qualifying_final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.qualifying")
# overwrite_partition(qualifying_final_df, 'f1_processed', 'qualifying', 'race_id')
merge_condition = "tgt.qualifying_id = src.qualifying_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df, "f1_processed", "qualifying", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# Read the parquet file
df = spark.read.format("delta").load(f"{processed_folder_path}/qualifying")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.qualifying;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1) FROM f1_processed.qualifying GROUP BY race_id ORDER BY race_id DESC;

# COMMAND ----------



# COMMAND ----------


