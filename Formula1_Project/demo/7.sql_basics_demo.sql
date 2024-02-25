-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM f1_processed.drivers limit 10;

-- COMMAND ----------

SELECT * FROM f1_processed.drivers WHERE nationality='British' and dob >= '1991-01-01' ORDER BY dob DESC;

-- COMMAND ----------


