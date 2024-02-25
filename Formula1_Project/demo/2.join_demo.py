# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").filter("circuit_id < 70").withColumnsRenamed({"name":"circuit_name"})
races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019").withColumnsRenamed({"name":"race_name"})

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Inner Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"inner") \
                              .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Outer Join

# COMMAND ----------

# Left Outer Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"left") \
                              .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# Right Outer Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"right") \
                              .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# Full Outer Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"full") \
                              .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Semi Joins
# MAGIC - This is very similar to Inner Join, but it only selects the columns from the left dataframe.

# COMMAND ----------

# Semi Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"semi") \
                              .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Anti Joins
# MAGIC - Semi join gives you what is available on both the dataframes.
# MAGIC - Anti Join gives you the opposite of semi join. It gives you everything from the left dataframe which is not found on the right dataframe.

# COMMAND ----------

# Anti Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"anti")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Cross Joins
# MAGIC - This gives you the Cartesian Product of the two dataframes.
# MAGIC - It's going to take every record from the left, and then going to join it to every record on the right and it's going to givbe you the product of the two.

# COMMAND ----------

# Anti Join
race_circuits_df = circuits_df.crossJoin(races_df)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------


