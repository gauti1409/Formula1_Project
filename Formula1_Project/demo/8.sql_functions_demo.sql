-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

SELECT *, concat(driver_ref, '-', code) as new_driver_reference FROM f1_processed.drivers;

-- COMMAND ----------

SELECT *, split(name, ' ')[0] as forename, split(name, ' ')[1] as surname FROM f1_processed.drivers;

-- COMMAND ----------

SELECT *, current_timestamp() FROM f1_processed.drivers;

-- COMMAND ----------

SELECT *, date_format(dob, 'dd-MM-yyyy'), date_add(dob,1) FROM f1_processed.drivers;

-- COMMAND ----------

SELECT COUNT(1) FROM f1_processed.drivers;

-- COMMAND ----------

SELECT max(dob) FROM drivers;

-- COMMAND ----------

SELECT * FROM f1_processed.drivers where dob='2000-05-11'

-- COMMAND ----------

SELECT nationality, count(1) FROM f1_processed.drivers GROUP BY nationality ORDER BY nationality;

-- COMMAND ----------

SELECT nationality, count(1) FROM f1_processed.drivers GROUP BY nationality HAVING count(1) > 100 ORDER BY nationality;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### WINDOW FUNCTIONS

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob DESC) AS AGE_RANK FROM f1_processed.drivers ORDER BY nationality, AGE_RANK;

-- COMMAND ----------


