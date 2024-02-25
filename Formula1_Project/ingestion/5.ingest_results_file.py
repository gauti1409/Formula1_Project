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

from pyspark.sql.functions import current_timestamp, lit
results_schema = """resultId INT, raceId INT, driverId INT, constructorId INT, number INT, grid INT, position INT, positionText STRING, positionOrder INT,
                  points FLOAT, laps INT, time STRING, milliseconds INT, fastestLap INT, rank INT, fastestLapTime STRING, fastestLapSpeed STRING,
                  statusId INT"""

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - Rename, add and drop the columns

# COMMAND ----------


results_renamed_df = results_df.withColumnsRenamed({"resultId":"result_id", "raceId":"race_id", "driverId":"driver_id", 
                                                    "constructorId":"constructor_id", "positionText":"position_text",
                                                    "positionOrder":"position_order", "fastestLap":"fastest_lap",
                                                    "fastestLapTime":"fastest_lap_time","fastestLapSpeed":"fastest_lap_speed"}) \
                                .withColumns({"file_date": lit(v_file_date), "data_source" : lit(v_data_source)}) \
                                .drop("statusId")

# COMMAND ----------

# MAGIC %md 
# MAGIC #####Step 3 - Add the ingestion date and drop the duplicates

# COMMAND ----------

results_final_df = add_ingestion_date(results_renamed_df)
results_deduped_df = results_final_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4 - Write the dataframe into Parquet file
# MAGIC - Note: Make sure to use collect for only small amount of data. Because what collect does is, it takes all the data and put it into the driver node's memory.

# COMMAND ----------

# MAGIC %md
# MAGIC - Method 1 for Incremental Load data

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"""ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})""")

# COMMAND ----------

# # results_final_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")
# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC - Method 2 for Incremental Load: Using insertInto method
# MAGIC 1. This is one of the methods to insert the records incrementally into the table.
# MAGIC 2. Required step: Spark expects that the last column in the dataframe in which records are getting inserted must be the PARTITIONED COLUMN, which is race_id in this case. So, we will have to put the race_id column at last position in the dataframe.
# MAGIC 3. We will use an IF-ELSE statement to create this logic:
# MAGIC   - If the table exists, that means the statement has already run and it's created the data for us. So going forward, we only want to add the new data for which the command is given in the IF Statement.
# MAGIC   - Else, it is the first execution of inserting the data. So, we can write our normal query and overwrite the data.
# MAGIC 4. We will also need to tell Spark that our Spark partition Overwrite mode is DYNAMIC. If we leave it as default and don't specify it as DYNAMIC, then it's called STATIC. In case of STATIC, the statement in IF clause is going to overwrite all of our data, which we don't want. So we will declare a command as below: 
# MAGIC   - spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
# MAGIC   - This means that when insertInto runs, it's going to find the partitions and only replace those partitions with the new data received. It's not going to overwrite the entire table.
# MAGIC
# MAGIC 5. This method is much more efficient because as supposed to going around and dropping all the partitions manually, we re letting spark to find the partition and overwrite it while it's trying to insert the data.

# COMMAND ----------

# %sql
# DROP TABLE f1_processed.results;

# COMMAND ----------

# overwrite_partition(results_final_df, "f1_processed", "results", "race_id")

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, "f1_processed", "results", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# Read the parquet file
df = spark.read.format("delta").load(f"{processed_folder_path}/results")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1) FROM f1_processed.results GROUP BY race_id ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, count(1) FROM f1_processed.results GROUP BY race_id, driver_id HAVING count(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC;

# COMMAND ----------


