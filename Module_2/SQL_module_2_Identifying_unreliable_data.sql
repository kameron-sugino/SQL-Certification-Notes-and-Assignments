/* ###
#No. 3 Identifying unreliable data*/
SELECT MAX(event_time),
MIN(event_time)
FROM dsv1069.events_201701;
/* data are only representative of Jan 2017
 */
#
SELECT MAX(event_time),
MIN(event_time)
FROM dsv1069.events_ex2;

/* #data from 2012-2016 
 */
SELECT platform,
MAX(event_time),
MIN(event_time)
FROM dsv1069.events_ex2
GROUP BY platform;

/* platform	max	min
Android	2016-12-31 23:15:03	2016-01-01 05:48:56
iOS	2016-12-31 23:02:56	2016-05-01 00:42:29
mobile web	2016-12-31 21:57:02	2013-01-13 21:10:30
server	2016-12-31 23:59:06	2012-11-23 00:07:10
web	2016-12-31 23:35:45	2012-12-12 01:18:09

#Mobile recording didn't start until 2016
 */
SELECT 
  date(event_time),
  platform,
  COUNT(*)
FROM dsv1069.events_ex2
GROUP BY
	date(event_time),
		platform
		
/* #Can also plot!
 */
#
SELECT *
FROM dsv1069.item_views_by_category_temp
;

/* #No, we shouldn't use this dataset because it's clearly a temp set and is not necessarily indicative of the data we want to pull
 */
#
SELECT *
FROM dsv1069.raw_events
;

/* #This table doesn't exist
 */

#
SELECT COUNT(*)
FROM dsv1069.orders;
/* #47420
 */
SELECT COUNT(*)
FROM dsv1069.users;
/* #117178
 */
SELECT COUNT(*)
FROM dsv1069.orders
JOIN dsv1069.users
ON orders.user_id = users.parent_user_id
;
/* #2604

#way fewer rows than there should be
 */

SELECT 
(CASE WHEN parent_user_id IS NULL 
  THEN 'no id'
  ELSE 'has id'
  END) AS ID_exist,
COUNT(*)
FROM dsv1069.users
GROUP BY ID_exist;

/* id_exist	count
has id	6408
no id	110770

#parent_user_id has a ton of null values id parameter is better to use */
