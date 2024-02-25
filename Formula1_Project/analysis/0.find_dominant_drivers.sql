-- Databricks notebook source
SELECT * FROM f1_presentation.calculated_race_results limit 10;

-- COMMAND ----------

--Calculate total points, avg points, total races based on the driver and rank them based on their avg_points
SELECT driver_name, COUNT(1) as total_races, SUM(calculated_points) as total_points, AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC; 

-- COMMAND ----------

--Calculate total points, avg points, total races based on the driver and rank them based on their avg_points
SELECT driver_name, COUNT(1) as total_races, SUM(calculated_points) as total_points, AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC; 

-- COMMAND ----------

--Calculate total points, avg points, total races based on the driver and rank them based on their avg_points
SELECT driver_name, COUNT(1) as total_races, SUM(calculated_points) as total_points, AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC; 

-- COMMAND ----------


