-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###### DROP ALL THE TABLES

-- COMMAND ----------

-- DROP DATABASE IF EXISTS f1_raw CASCADE;
DROP DATABASE IF EXISTS f1_processed CASCADE;
DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

-- CREATE DATABASE IF NOT EXISTS f1_raw
-- LOCATION "/mnt/formulastorage14dl/raw";

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formulastorage14dl/processed";

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formulastorage14dl/presentation";

-- COMMAND ----------


