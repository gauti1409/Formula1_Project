-- Databricks notebook source
--Calculate total points, avg points, total races based on the driver and rank them based on their avg_points
SELECT team_name, COUNT(1) as total_races, SUM(calculated_points) as total_points, AVG(calculated_points) as avg_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) as team_rank
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC;

-- COMMAND ----------

SELECT race_year, team_name, COUNT(1) as total_races, SUM(calculated_points) as total_points, AVG(calculated_points) as avg_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) as team_rank
FROM f1_presentation.calculated_race_results
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC;

-- COMMAND ----------

WITH CTE_1 AS (SELECT team_name, COUNT(1) as total_races, SUM(calculated_points) as total_points, AVG(calculated_points) as avg_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) as team_rank
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC),
CTE_2 AS (SELECT race_year, team_name, COUNT(1) as total_races, SUM(calculated_points) as total_points, AVG(calculated_points) as avg_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) as team_rank
FROM f1_presentation.calculated_race_results
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC)
SELECT CTE_2.race_year, CTE_2.team_name, CTE_2.total_races, CTE_2.total_points, CTE_2.avg_points FROM CTE_2 
WHERE CTE_2.team_name in (SELECT CTE_1.team_name FROM CTE_1 WHERE CTE_1.team_rank <= 5);

-- COMMAND ----------

WITH CTE_1 AS (SELECT team_name, COUNT(1) as total_races, SUM(calculated_points) as total_points, AVG(calculated_points) as avg_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) as team_rank
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC),
CTE_2 AS (SELECT race_year, team_name, COUNT(1) as total_races, SUM(calculated_points) as total_points, AVG(calculated_points) as avg_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) as team_rank
FROM f1_presentation.calculated_race_results
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC)
SELECT CTE_2.race_year, CTE_2.team_name, CTE_2.total_races, CTE_2.total_points, CTE_2.avg_points FROM CTE_2 
WHERE CTE_2.team_name in (SELECT CTE_1.team_name FROM CTE_1 WHERE CTE_1.team_rank <= 5);

-- COMMAND ----------


