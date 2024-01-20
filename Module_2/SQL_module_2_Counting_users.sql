/* ######
#No. 4 Counting Users*/
SELECT (date(created_at)) AS users_per_day,
COUNT(*)
FROM dsv1069.users
GROUP BY users_per_day
ORDER BY users_per_day;

/* #Not sure of other ways to report this, but the users per day have been steadily climbing since 2013, reaching around 50/d in 2015 and plateuing around 100/d in 2018
 */
SELECT
  date(deleted_at)	AS day, 
  COUNT(*)	AS users
FROM dsv1069.users
WHERE deleted_at IS NOT NULL
AND (id <> parent_user_id OR parent_user_id IS NULL)
GROUP BY date(deleted_at)
/* 
#I think you'd want this to read IS NOT NULL after WHERE to count the deleted users


#I think I'm missing something building this table */
SELECT 
  date(created_at)	AS new_user,
  COUNT(created_at)	AS count_new_user,
  COUNT(deleted_at)	AS count_del_user,
  COUNT(merged_at)	AS count_mer_user
FROM dsv1069.users
GROUP BY 
  new_user;
  
/* #I was! You join by date in three subqeuries
#need to coalesce the numeric items to replace NULL with 0 so I can do math on them */
SELECT 
  new.day AS date,
  COALESCE(new.new_users,0) AS new_user,
  COALESCE(del.users,0) AS deleted_user,
  COALESCE(mer.users,0) AS merged_user,
  (new.new_users - COALESCE(del.users,0) - COALESCE(mer.users,0)) AS net_new_user
FROM
  (SELECT (date(created_at)) AS day,
  COUNT(*) new_users
  FROM dsv1069.users
  GROUP BY day) new
LEFT JOIN 
  (SELECT
    date(deleted_at)	AS day, 
    COUNT(*)	AS users
  FROM dsv1069.users
  WHERE (id <> parent_user_id OR parent_user_id IS NULL)
  GROUP BY date(deleted_at)) del 
ON 
  new.day = del.day
LEFT JOIN 
  (SELECT
    date(merged_at)	AS day, 
    COUNT(*)	AS users
  FROM dsv1069.users
  WHERE deleted_at IS NOT NULL
  GROUP BY date(merged_at)) mer
ON 
  new.day = mer.day
  
  
/*
#This should pull all dates whether they have a new_user entry ot not 
#basically just move the join to dr.date = new.day
#I thought you'd have to make all the comparisons to dr.date, but it seems that it works if you LEFT JOIN dr.date = new.day at the first step and then join to the new.day since it now contains all the dates inherently
*/

SELECT 
  dr.date AS date,
  COALESCE(new.new_users,0) AS new_user,
  COALESCE(del.users,0) AS deleted_user,
  COALESCE(mer.users,0) AS merged_user,
  (COALESCE(new.new_users,0) - COALESCE(del.users,0) - COALESCE(mer.users,0)) AS net_new_user
FROM dsv1069.dates_rollup dr
LEFT JOIN
  (SELECT (date(created_at)) AS day,
  COUNT(*) new_users
  FROM dsv1069.users
  GROUP BY day) new
ON 
  dr.date = new.day
LEFT JOIN 
  (SELECT
    date(deleted_at)	AS day, 
    COUNT(*)	AS users
  FROM dsv1069.users
  WHERE (id <> parent_user_id OR parent_user_id IS NULL)
  GROUP BY date(deleted_at)) del 
ON 
  new.day = del.day
LEFT JOIN 
  (SELECT
    date(merged_at)	AS day, 
    COUNT(*)	AS users
  FROM dsv1069.users
  WHERE deleted_at IS NOT NULL
  GROUP BY date(merged_at)) mer
ON 
  new.day = mer.day
  
  
 

/* #creating tables
 */
 
CREATE TABLE 'view_item_events' (
	event_id 	VARCHAR(32) NOT NULL PRIMARY KEY,
	event_time 	VARCHAR(26),
	user_id		INT(10),
	platform	VARCHAR(10),
	item_id		INT(10),
	referrer	VARCHAR(17)
):

INSERT INTO
'view_item_events'

SELECT 
	event_id,
	event_time,
	user_id,
	platform,
    MAX(CASE WHEN parameter_name = 'item_id'
      THEN CAST(parameter_value AS INT)
      ELSE NULL
      END) AS item_id,
    MAX(CASE WHEN parameter_name = 'referrer'
      THEN parameter_value
      ELSE NULL
      END) AS referrer
FROM dsv1069.events 
WHERE event_name = 'view_item'
GROUP BY
	event_id,
	event_time,
	user_id,
	platform;
	
/* Can also just make the table, but returns unformatted columns  */
CREATE TABLE
	view_item_events_1
	AS 
	SELECT event_id,
	event_time,
	user_id,
	platform,
  MAX(CASE WHEN parameter_name = 'item_id'
    THEN CAST(parameter_value AS INT)
    ELSE NULL
    END) AS item_id,
  MAX(CASE WHEN parameter_name = 'referrer'
    THEN parameter_value
    ELSE NULL
    END) AS referrer
FROM dsv1069.events 
WHERE event_name = 'view_item'
GROUP BY
	event_id,
	event_time,
	user_id,
	platform;
	
	
/* Automating tasks and pulling data: liquid tags */
/* names a variable to be called later */
{% assign ds = '2018-01-01' %}
SELECT
id
FROM users
WHERE created_at <= '{{ds}}'
