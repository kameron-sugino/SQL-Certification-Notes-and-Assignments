/* #Assignment 1 - Queries in Spark SQL
 */
#mount dataset
%run ../Includes/Classroom-Setup

#create table
-- TODO
CREATE TABLE IF NOT EXISTS fireincidents
USING CSV
OPTIONS(
  header "true",
  path "/mnt/davis/fire-incidents/fire-incidents-2016.csv",
  inferSchema "true"
)

/* #First value of "Incident Number" is 16000003 */
SELECT 
  *
FROM fireincidents
LIMIT 10;

/* #pull only incidents on april 4th 2016 */
SELECT 
  *
FROM fireincidents
WHERE DATE(`Incident Date`) = DATE("2016-04-04");
/* 
#pull incidents that happened on april 4th 2016 or Sept. 27th 2016 */
SELECT 
  *
FROM fireincidents
WHERE DATE(`Incident Date`) = DATE("2016-04-04")
OR DATE(`Incident Date`) = DATE("2016-09-27");

/* #pull incidents that happened on april 4th 2016 or Sept. 27th 2016 and where "Station Area" is >20 */
SELECT 
  *
FROM fireincidents
WHERE `Station Area` > 20 AND
  (
    DATE(`Incident Date`) = DATE("2016-04-04")
  OR DATE(`Incident Date`) = DATE("2016-09-27")
  );

/* #count incidents on april 4th 2016; 80 incidents */
SELECT 
  COUNT(*)
FROM fireincidents
WHERE DATE(`Incident Date`) = DATE("2016-04-04");

/* #Return the total counts by Ignition Cause. Be sure to return the field Ignition Cause as well. */
SELECT
`Ignition Cause`,
  COUNT(*)
FROM fireincidents
GROUP BY `Ignition Cause`;

/* #Return the total counts by Ignition Cause sorted in ascending order.
 */
SELECT
`Ignition Cause`,
  COUNT(*)
FROM fireincidents
GROUP BY `Ignition Cause`
ORDER BY COUNT(*) ASC;

/* #Return the total counts by Ignition Cause sorted in descending order. */
SELECT
`Ignition Cause`,
  COUNT(*)
FROM fireincidents
GROUP BY `Ignition Cause`
ORDER BY COUNT(*) DESC;


/* #Create the table fireCalls if it doesn't already exist. The path is as follows: */
CREATE TABLE IF NOT EXISTS firecalls
USING CSV
OPTIONS(
  header "true",
  path "/mnt/davis/fire-calls/fire-calls-truncated-comma.csv",
  inferSchema "true"
)

/* #Join the two tables on Battalion by performing an inner join. */
SELECT
	*
FROM firecalls
JOIN fireincidents
	ON firecalls.Battalion = fireincidents.Battalion


/* #Count the total incidents from the two tables joined on Battalion.
#counted the unique incident numbers: 227770 */
SELECT
	COUNT(DISTINCT firecalls.`Incident Number`) AS Distinct_Incidences
FROM firecalls
JOIN fireincidents
	ON firecalls.Battalion = fireincidents.Battalion
