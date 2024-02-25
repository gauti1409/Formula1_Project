# Databricks notebook source
# Declaring widgets to parameterize the notebook. Parameters helps us to use the entire notebook.
from pyspark.sql.functions import sum, count, when, col, rank, desc
dbutils.widgets.text("parameter_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("parameter_file_date")

# COMMAND ----------

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
          (
              race_year INT,
              team_name STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
          )
          USING DELTA
          """)

# COMMAND ----------

#Calculating the dominant driver and the dominant team throughout the race history
spark.sql(f"""
          CREATE OR REPLACE TEMP VIEW race_results_updated
          AS 
          SELECT races.race_year, constructors.name as team_name, drivers.driver_id, drivers.name as driver_name, 
                 races.race_id, results.position, results.points, 11 - results.position as calculated_points
          FROM f1_processed.results
          JOIN f1_processed.drivers on (results.driver_id = drivers.driver_id)
          JOIN f1_processed.constructors ON (constructors.constructors_id = results.constructor_id)
          JOIN f1_processed.races on (results.race_id = races.race_id)
          WHERE results.position <= 10
          AND results.file_date = '{v_file_date}' 
          """)

# COMMAND ----------

spark.sql(f"""
                MERGE INTO f1_presentation.calculated_race_results as tgt
                USING race_results_updated as updt
                ON (tgt.driver_id = updt.driver_id AND tgt.race_id = updt.race_id)
                WHEN MATCHED THEN 
                UPDATE SET tgt.position = updt.position,
                            tgt.points = updt.points,
                            tgt.calculated_points = updt.calculated_points,
                            tgt.updated_date = current_timestamp
                WHEN NOT MATCHED 
                THEN INSERT (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, created_date) 
                    VALUES (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, current_timestamp)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM race_results_updated WHERE race_year=2021;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM race_results_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM f1_presentation.calculated_race_results;

# COMMAND ----------


