# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to a delta lake(managed table)
# MAGIC 2.  Write data to a delta lake(external table)
# MAGIC 3. Read data from delta lake(Table)
# MAGIC 4. Read data from delta lake(File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo_delta
# MAGIC LOCATION '/mnt/formulastorage14dl/demo-delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DATABASE f1_demo_delta

# COMMAND ----------

results_df = spark.read.option("inferSchema",True).json("/mnt/formulastorage14dl/raw/2021-03-28/results.json")

# COMMAND ----------

# Saving the table in the DB in Unity catalog
results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo_delta.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.results_managed;

# COMMAND ----------

# Saving the table in the file location
results_df.write.format("delta").mode("overwrite").save("/mnt/formulastorage14dl/demo-delta/results_external")

# COMMAND ----------

# %sql 
# DROP DATABASE f1_demo_delta CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo_delta.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formulastorage14dl/demo-delta/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.results_external;

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/formulastorage14dl/demo-delta/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo_delta.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo_delta.results_partitioned;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update from Delta Lake Table
# MAGIC 2. Delete from Delta Lake Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo_delta.results_managed 
# MAGIC SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formulastorage14dl/demo-delta/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(condition = "position <= 10", set = { "points": "21-position" })

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo_delta.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formulastorage14dl/demo-delta/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete(condition = "points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert using merge
# MAGIC - A merge statement gives you the ability to insert any new records being received.
# MAGIC - Update any new records for which new data has been received.
# MAGIC - And if you have any delete request, then you can apply that delete as well. 
# MAGIC - And you do all above operations in one statement called as UPSERT.

# COMMAND ----------

drivers_day1_df = spark.read.option("inferSchema", True).json("/mnt/formulastorage14dl/raw/2021-03-28/drivers.json").filter("driverId<=10") \
                            .select("driverId", "dob", "name.forename", "name.surname")
drivers_day1_df.createOrReplaceTempView("drivers_day1_df")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day2_df = spark.read.option("inferSchema", True).json("/mnt/formulastorage14dl/raw/2021-03-28/drivers.json") \
                            .filter("driverId BETWEEN 6 AND 15") \
                            .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))
drivers_day2_df.createOrReplaceTempView("drivers_day2_df")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day3_df = spark.read.option("inferSchema", True).json("/mnt/formulastorage14dl/raw/2021-03-28/drivers.json") \
                            .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
                            .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo_delta.drivers_merge(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo_delta.drivers_merge as tgt
# MAGIC USING drivers_day1_df as updt
# MAGIC ON tgt.driverId = updt.driverId
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET tgt.dob = updt.dob,
# MAGIC              tgt.forename = updt.forename,
# MAGIC              tgt.surname = updt.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo_delta.drivers_merge as tgt
# MAGIC USING drivers_day2_df as updt
# MAGIC ON tgt.driverId = updt.driverId
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET tgt.dob = updt.dob,
# MAGIC              tgt.forename = updt.forename,
# MAGIC              tgt.surname = updt.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.drivers_merge ORDER BY driverId ASC;

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

deltaTable = DeltaTable.forPath(spark, '/mnt/formulastorage14dl/demo-delta/drivers_merge')

deltaTable.alias('tgt').merge(drivers_day3_df.alias('updt'),'tgt.driverId = updt.driverId') \
          .whenMatchedUpdate(set =
                             {"dob" : "updt.dob","forename" : "updt.forename",
                              "surname" : "updt.surname","updatedDate" : current_timestamp()}) \
          .whenNotMatchedInsert(values =
                                {"driverId": "updt.driverId","dob": "updt.dob","forename": "updt.forename","surname": "updt.surname",
                                 "createdDate": current_timestamp()}) \
          .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.drivers_merge ORDER BY driverId ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC - History and Versioning
# MAGIC - Time travel
# MAGIC - Vacuum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo_delta.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.drivers_merge VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.drivers_merge VERSION AS OF 2;                                                                                              

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.drivers_merge TIMESTAMP AS OF '2024-02-24T12:59:17.000+00:00';  

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", "2024-02-24T12:59:17.000+00:00").load("/mnt/formulastorage14dl/demo-delta/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo_delta.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.drivers_merge TIMESTAMP AS OF '2024-02-24T12:59:17.000+00:00';  

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo_delta.drivers_merge RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.drivers_merge TIMESTAMP AS OF '2024-02-24T12:59:17.000+00:00';  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo_delta.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.drivers_merge VERSION AS OF 6;  

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo_delta.drivers_merge as tgt
# MAGIC USING f1_demo_delta.drivers_merge VERSION AS OF 6 as src
# MAGIC ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESC HISTORY f1_demo_delta.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Transaction Logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo_delta.drivers_txn(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo_delta.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo_delta.drivers_txn
# MAGIC SELECT * FROM f1_demo_delta.drivers_merge
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo_delta.drivers_txn

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo_delta.drivers_convert_to_delta(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo_delta.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo_delta.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo_delta.drivers_convert_to_delta;

# COMMAND ----------

df = spark.table("f1_demo_delta.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/formulastorage14dl/demo-delta/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/formulastorage14dl/demo-delta/drivers_convert_to_delta_new`

# COMMAND ----------


