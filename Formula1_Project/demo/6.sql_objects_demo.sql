-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Lesson objectives
-- MAGIC 1. Spark SQL Documentation
-- MAGIC 2. Create DB Demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW Command
-- MAGIC 5. DESCRIBE Command
-- MAGIC 6. Find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES IN DEMO;

-- COMMAND ----------

USE DEMO;
SELECT current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Managed Tables - Learning Objectives
-- MAGIC 1. Create managed tables using Python and SQL
-- MAGIC 2. Effect of dropping a managed table
-- MAGIC 3. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESCRIBE EXTENDED race_results_python;

-- COMMAND ----------

SELECT * FROM demo.race_results_python WHERE race_year= 2020 limit 15;

-- COMMAND ----------

CREATE TABLE race_results_sql AS (SELECT * FROM demo.race_results_python);

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Learning Objectives
-- MAGIC 1. Create external table using python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name VARCHAR(255),
race_date TIMESTAMP,
circuit_location VARCHAR(255),
driver_name VARCHAR(255),
driver_number INT,
driver_nationality VARCHAR(255),
team VARCHAR(255),
grid INT,
fastest_lap INT,
race_time VARCHAR(255),
points FLOAT,
position INT,
created_date TIMESTAMP)
USING PARQUET
LOCATION "/mnt/formulastorage14dl/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql SELECT * FROM demo.race_results_ext_py WHERE race_year=2020;

-- COMMAND ----------

SELECT COUNT(1) FROM demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES in demo

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Views on tables
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Gloabl temp view
-- MAGIC 3. Create Permanent view

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS 
SELECT * FROM demo.race_results_python WHERE race_year=2020

-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE global TEMP VIEW gb_race_results
AS 
SELECT * FROM demo.race_results_python WHERE race_year=2012

-- COMMAND ----------

SELECT * FROM global_temp.gb_race_results

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES in global_temp;

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS 
SELECT * FROM demo.race_results_python WHERE race_year=2000;

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------


