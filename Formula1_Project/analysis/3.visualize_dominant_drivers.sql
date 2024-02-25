-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

--Calculate total points, avg points, total races based on the driver and rank them based on their avg_points
SELECT driver_name, COUNT(1) as total_races, SUM(calculated_points) as total_points, AVG(calculated_points) as avg_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) as driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC;

-- COMMAND ----------

SELECT race_year, driver_name, COUNT(1) as total_races, SUM(calculated_points) as total_points, AVG(calculated_points) as avg_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) as driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC;

-- COMMAND ----------

WITH CTE_1 AS (SELECT driver_name, COUNT(1) as total_races, SUM(calculated_points) as total_points, AVG(calculated_points) as avg_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) as driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC),
CTE_2 AS (SELECT race_year, driver_name, COUNT(1) as total_races, SUM(calculated_points) as total_points, AVG(calculated_points) as avg_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) as driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC)
SELECT CTE_2.race_year, CTE_2.driver_name, CTE_2.total_races, CTE_2.total_points, CTE_2.avg_points FROM CTE_2 
WHERE CTE_2.driver_name in (SELECT CTE_1.driver_name FROM CTE_1 WHERE CTE_1.driver_rank <= 10);

-- COMMAND ----------

WITH CTE_1 AS (SELECT driver_name, COUNT(1) as total_races, SUM(calculated_points) as total_points, AVG(calculated_points) as avg_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) as driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC),
CTE_2 AS (SELECT race_year, driver_name, COUNT(1) as total_races, SUM(calculated_points) as total_points, AVG(calculated_points) as avg_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) as driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC)
SELECT CTE_2.race_year, CTE_2.driver_name, CTE_2.total_races, CTE_2.total_points, CTE_2.avg_points FROM CTE_2 
WHERE CTE_2.driver_name in (SELECT CTE_1.driver_name FROM CTE_1 WHERE CTE_1.driver_rank <= 10);

-- COMMAND ----------


