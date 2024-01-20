/* No. 9 Test Assignments

-
*/


/* Exercise 1: Figure out how many tests we have running right now

-There are 6 parameters all with fairly large n's:

SELECT 
  parameter_value,
  COUNT(parameter_value)
FROM 
  dsv1069.events
WHERE event_name = 'test_assignment'
GROUP BY parameter_value

parameter_value	count
0	82693
1	79797
4	11890
5	68563
6	43390
7	38647


Seems like they actually wanted us to filter by another column...not sure how I could've know this, but sure.
SELECT 
  parameter_value,
  COUNT(parameter_value)
FROM 
  dsv1069.events
WHERE event_name = 'test_assignment'
AND parameter_name = 'test_id'
GROUP BY parameter_value
parameter_value	count
4	11890
5	68563
6	43390
7	38647

*/





/* Exercise 2: Check for potential problems with test assignments. For example Make sure there is no data obviously missing (This is an open ended question)

If we check the unique users vs the total number of rows, we can see there are many more counts than there are unique users

SELECT 
  parameter_value,
  COUNT(DISTINCT user_id),
  COUNT(*)
FROM 
  dsv1069.events
WHERE event_name = 'test_assignment'
GROUP BY parameter_value

parameter_value	count_duplicate_column_name_1	count
0	63725	82693
1	62534	79797
4	11890	11890
5	68563	68563
6	43390	43390
7	38647	38647

-so it looks like we have duplicate user_ids in only parameter 0 and 1


Are the users part of different parameters?

SELECT 
  AVG(parameter_count)
FROM 
  (SELECT 
    COUNT(parameter_value) AS parameter_count,
    user_id
  FROM 
    dsv1069.events
  WHERE event_name = 'test_assignment'
  GROUP BY user_id) user_parameter
  
avg
3.340254080499938

-Yup! The math checks out (3.34 * 97292 ~ 324980)


How many users are included in multiple parameters?

SELECT
  parameter_count,
  COUNT(*)
FROM 
  (SELECT 
    COUNT(parameter_value) AS parameter_count,
    user_id
  FROM 
    dsv1069.events
  WHERE event_name = 'test_assignment'
  GROUP BY user_id) user_parameter
GROUP BY parameter_count

parameter_count	count
2	46076
4	38198
6	12054
8	964

So, it looks like every user is in at least 2 parameters, but this includes duplicates (since parameters 0 and 1 had duplicate user IDs and there are 964 entried with 8 parameter counts)



If we exclude the parameters 1 and 0, no users have multiple assignments

SELECT 
    COUNT(*),
    parameter_value,
    user_id
  FROM 
    dsv1069.events
  WHERE event_name = 'test_assignment'
  AND parameter_name = 'test_id'
  GROUP BY 
  user_id,
  parameter_value
ORDER BY COUNT(*) DESC


*/


/* Exercise 3: Write a query that returns a table of assignment events.Please include all of the relevant parameters as columns (Hint: A previous exercise as a template)

SELECT 
  *
FROM
  (SELECT 
  	ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY parameter_value) AS user_parameters,
  	event_time,
  	user_id,
  	parameter_value,
	test_id
  FROM 
    dsv1069.events 
  WHERE event_name = 'test_assignment') parameter_summary
  
  
How they did it:

SELECT event_id,
event_time,
user_id,
platform,
  MAX(CASE WHEN parameter_name = 'test_id'
    THEN CAST(parameter_value AS INT)
    ELSE NULL
    END) AS test_id,
  MAX(CASE WHEN parameter_name = 'test_assignment'
    THEN parameter_value
    ELSE NULL
    END) AS test_assignment
FROM dsv1069.events 
WHERE event_name = 'test_assignment'
GROUP BY
	event_id,
	event_time,
	user_id,
	platform;
	
I don't think they gave a great explaination of the data set's structure because I have no idea how we are supposed to know what test_id and test_assignment refer to...

It seems that the assignment helps sort out the user parameter tests, but I don't quite get how this is the case. In my queries there are clearly people with multiple assignments independent of tests 0 and 1
*/


/* Exercise 4: Check for potential assignment problems with test_id 5. Specifically, make sure users are assigned only one treatment group.

I think the code from 2 answers the same question?

SELECT 
    COUNT(*),
    parameter_value,
    user_id
  FROM 
    dsv1069.events
  WHERE event_name = 'test_assignment'
  AND parameter_name = 'test_id'
  GROUP BY 
  user_id,
  parameter_value
ORDER BY COUNT(*) DESC


How they did it:
SELECT
  user_id,
  test_id,
  COUNT(*)
FROM
  (SELECT event_id,
  event_time,
  user_id,
  platform,
    MAX(CASE WHEN parameter_name = 'test_id'
      THEN CAST(parameter_value AS INT)
      ELSE NULL
      END) AS test_id,
    MAX(CASE WHEN parameter_name = 'test_assignment'
      THEN parameter_value
      ELSE NULL
      END) AS test_assignment
  FROM dsv1069.events 
  WHERE event_name = 'test_assignment'
  GROUP BY
  	event_id,
  	event_time,
  	user_id,
  	platform) test_assignment
GROUP BY 
  test_id,
  user_id
ORDER BY COUNT(*) DESC

*/
