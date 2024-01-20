/* 
Assignment 2 - Spark Internals
*/

#How many fire calls are in our fireCalls table?
#240613
SELECT
  Count(*)
FROM fireCalls

#How large is our fireCalls dataset in memory?
#59.3 
CACHE TABLE fireCalls


#Which "Unit Type" is the most common?
#ENGINE 92828
SELECT
  `Unit Type`,
  Count(*) AS count
FROM fireCalls
GROUP BY `Unit Type`
ORDER BY count DESC

#**What type of transformation, wide or narrow, did the GROUP BY and ORDER BY queries result in? **
#Wide

#Looking at the query below, how many tasks are in the last stage of the last job?
#1 task in the last job

SET spark.sql.shuffle.partitions=8;

SELECT `Neighborhooods - Analysis Boundaries`, AVG(Priority) as avgPriority
FROM fireCalls
GROUP BY `Neighborhooods - Analysis Boundaries`
