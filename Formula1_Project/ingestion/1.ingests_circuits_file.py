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
# MAGIC **Step 1 - Read the csv file using the Spark dataframe reader**

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit, current_timestamp
circuits_schema = StructType(fields=[
    StructField("circuitid", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read.options(header= True).schema(circuits_schema).csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2 - Select only the required columns**
# MAGIC - Note: The 1st way will only let you select the columns but the other 3 ways(2,3,4) will also let you apply any functions on top of those columns like rename etc. to do any changes or modifications with the respective columns.

# COMMAND ----------

# 1st way 
# circuits_selected_df = circuits_df.select("circuitid","circuitRef","name","location","country","lat","lng","alt")

#2nd way
# circuits_selected_df = circuits_df.select(circuits_df.circuitid, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country,
#                                           circuits_df.lat, circuits_df.lng, circuits_df.alt)

#3rd way
# circuits_selected_df = circuits_df.select(circuits_df["circuitid"], circuits_df["circuitRef"], circuits_df["name"],
#                                           circuits_df["location"], circuits_df["country"], circuits_df["lat"],circuits_df["lng"], circuits_df["alt"])

# COMMAND ----------

#4th way
circuits_selected_df = circuits_df.select(col("circuitid"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3 - Rename the columns as required**
# MAGIC

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnsRenamed({"circuitid":"circuit_id","circuitRef":"circuit_ref","lat":"latitude",
                                                               "lng":"longitude","alt":"altitude"}) \
                                          .withColumns({"file_date": lit(v_file_date), "data_source": lit(v_data_source)})

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 4- Adding the new column ingestion_date for holding the current Timestamp**

# COMMAND ----------

# Remember, to pass a literal value to a column, we will have to use lit function to wrap around the literal value which then can be converted to column object
# circuits_final_df = circuits_renamed_df.withColumns({"ingestion_date":current_timestamp(),"env":lit("production")})

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #####Step 5- Write data to data lake as parquet file

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# Reading the parquet file to see the data
df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


