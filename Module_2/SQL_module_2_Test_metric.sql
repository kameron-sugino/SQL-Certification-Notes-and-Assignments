/* No. 10 Create a Test Metric
-
*/


/* Exercise 1:
--Using the table from Exercise 4.3 and compute a metric that measures
--Whether a user created an order after their test assignment

--Requirements: Even if a user had zero orders, we should have a row that counts
-- their number of orders as zero
--If the user is not in the experiment they should not be included 


-It looks like there's basically no difference in order volume
*/

SELECT
  COUNT(DISTINCT 
  	(CASE WHEN orders.created_at > test_assignment.event_time 
  	THEN invoice_id ELSE NULL
  	END)) AS orders_after_assignment,
  COUNT(DISTINCT 
  	(CASE WHEN orders.created_at > test_assignment.event_time 
  	THEN line_item_id ELSE NULL
  	END)) AS items_after_assignment,
  SUM(
  	(CASE WHEN orders.created_at > test_assignment.event_time
  	THEN price ELSE 0
  	END)) AS total_revenue,
  test_assignment.test_assignment,
  test_assignment.test_id
  --,test_assignment.user_id
  --DATE(orders.paid_at)
FROM
  (SELECT
  	event_id,
  	event_time,
  	user_id,
  	--platform,
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
  	event_time
	--,user_id
	) test_assignment
LEFT JOIN 
  dsv1069.orders
ON test_assignment.user_id = orders.user_id
GROUP BY 
  test_assignment.test_assignment,
  test_assignment.test_id
  --,test_assignment.user_id
  --DATE(paid_at)
ORDER BY 
  test_id,
  test_assignment



/* Exercise 2:
--Using the table from the previous exercise, add the following metrics
--1) the number of orders/invoices
--2) the number of items/line-items ordered
--3) the total revenue from the order after treatment 

-done I think? I sorta did Q.1 wrong by creating Q.2 instead	
*/

SELECT
  COUNT(DISTINCT 
  	(CASE WHEN orders.created_at > test_assignment.event_time 
  	THEN invoice_id ELSE NULL
  	END)) AS orders_after_assignment,
  COUNT(DISTINCT 
  	(CASE WHEN orders.created_at > test_assignment.event_time 
  	THEN line_item_id ELSE NULL
  	END)) AS items_after_assignment,
  SUM(
  	(CASE WHEN orders.created_at > test_assignment.event_time
  	THEN price ELSE 0
  	END)) AS total_revenue,
  test_assignment.test_assignment,
  test_assignment.test_id
  --,  test_assignment.user_id
  --DATE(orders.paid_at)
FROM
  (SELECT
  	event_id,
  	event_time,
  	user_id,
  	--platform,
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
  	user_id
	) test_assignment
LEFT JOIN 
  dsv1069.orders
ON test_assignment.user_id = orders.user_id
GROUP BY 
  test_assignment.test_assignment,
  test_assignment.test_id
  --, test_assignment.user_id
  --DATE(paid_at)
ORDER BY 
  test_id,
  test_assignment



/* No.11 Analyzing Results

*/


/* Exercise 1: Use the order_binary metric from the previous exercise, count the number of users per treatment group for test_id = 7, and count the number of users with orders (for test_id 7)

test_assignment	count	sum
0	19376	2521
1	19271	2633

You gotta hard code in the CASE() to retrieve null values for the total number of cases
*/

SELECT 
  test_assignment,
  COUNT(user_id),
  SUM(orders_binary)
FROM
  (SELECT
    MAX(
      CASE WHEN orders.created_at > test_assignment.event_time 
    	THEN 1 ELSE 0
    	END) AS orders_binary,
    test_assignment.test_assignment,
    test_assignment.test_id
    ,  test_assignment.user_id
    --DATE(orders.paid_at)
  FROM
    (SELECT
    	event_id,
    	event_time,
    	user_id,
    	--platform,
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
    	user_id
  	) test_assignment
  LEFT JOIN 
    dsv1069.orders
  ON test_assignment.user_id = orders.user_id
  GROUP BY 
    test_assignment.test_assignment,
    test_assignment.test_id
    , test_assignment.user_id
    --DATE(paid_at)
  ) orders_binary
WHERE test_id = 7
GROUP BY test_assignment

/* Exercise 2: Create a new term view binary metric. Count the number of users per treatment group, and count the number of users with views (for test_id 7)

test_assignment	count	sum
0	19376	10290
1	19271	10271

Same as above, but you join by a different table and count the views
*/

SELECT 
  test_assignment,
  COUNT(user_id),
  SUM(views_binary)
FROM
  (SELECT
    MAX(
      CASE WHEN view_event.event_time > test_assignment.event_time 
    	THEN 1 ELSE 0
    	END) AS views_binary,
    test_assignment.test_assignment,
    test_assignment.test_id
    ,  test_assignment.user_id
    --DATE(orders.paid_at)
  FROM
    (SELECT
    	event_id,
    	event_time,
    	user_id,
    	--platform,
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
    	user_id
  	) test_assignment
  LEFT JOIN 
    (SELECT 
      *
    FROM 
      dsv1069.events 
    WHERE event_name = 'view_item'
    ) view_event
  ON test_assignment.user_id = view_event.user_id
  GROUP BY 
    test_assignment.test_assignment,
    test_assignment.test_id
    , test_assignment.user_id
    --DATE(paid_at)
  ) orders_binary
WHERE test_id = 7
GROUP BY test_assignment

/* Exercise 3: Alter the result from EX 2, to compute the users who viewed an item WITHIN 30 days of their treatment event

test_assignment	count	views_binary	views_binary_30d
0	19376	10290	245
1	19271	10271	237

DATE_PART allows us to take the 'day' and subtract it out; I think this is more innaccurate than using DATE since it only puls the day of the month; there could be cases where a view occurs years later but due to chance the difference in day is <=30--not sure if this is the case, but when I tested it, there were around 20 fewer instances of 30d views for each assignment
*/ 

SELECT 
  test_assignment,
  COUNT(user_id),
  SUM(views_binary) AS views_binary,
  SUM(views_binary_30d) AS views_binary_30d
FROM
  (SELECT
    MAX(
      CASE WHEN view_event.event_time > test_assignment.event_time 
    	THEN 1 ELSE 0
    	END) AS views_binary,
    MAX(
      CASE WHEN view_event.event_time > test_assignment.event_time 
            AND DATE_PART('day', view_event.event_time - test_assignment.event_time) <= 30 
    	THEN 1 ELSE 0
    	END) AS views_binary_30d,
    test_assignment.test_assignment,
    test_assignment.test_id
    ,  test_assignment.user_id
    --DATE(orders.paid_at)
  FROM
    (SELECT
    	event_id,
    	event_time,
    	user_id,
    	--platform,
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
    	user_id
  	) test_assignment
  LEFT JOIN 
    (SELECT 
      *
    FROM 
      dsv1069.events 
    WHERE event_name = 'view_item'
    ) view_event
  ON test_assignment.user_id = view_event.user_id
  GROUP BY 
    test_assignment.test_assignment,
    test_assignment.test_id
    , test_assignment.user_id
    --DATE(paid_at)
  ) orders_binary
WHERE test_id = 7
GROUP BY test_assignment

/* Exercise 4:
Create the metric invoices (this is a mean metric, not a binary metric) and for test_id = 7
----The count of users per treatment group
----The average value of the metric per treatment group
----The standard deviation of the metric per treatment group

test_assignment	test_id	total_users	total_orders	avg_revenue	sd_revenue
0	4	7210	1060	44.053290568654646	189.12282785537965
1	4	4680	670	39.70155982905981	183.0709522084885
0	5	34420	4977	40.865579023823386	179.05955121121988
1	5	34143	5075	41.455876753653584	181.35655196567515
0	6	21687	3164	40.91301955088304	178.81235861842026
1	6	21703	3190	40.52631456480671	178.44008165940662
0	7	19376	2521	36.227406843517755	163.42049254469285
1	7	19271	2633	37.68206138757719	171.46723655175987
*/

SELECT 
  test_assignment,
  test_id,
  COUNT(user_id) AS total_users,
  SUM(orders_binary) AS total_orders,
  AVG(total_revenue) AS avg_revenue,
  STDDEV(total_revenue) AS sd_revenue
FROM
  (SELECT
    MAX(
      CASE WHEN orders.created_at > test_assignment.event_time 
    	THEN 1 ELSE 0
    	END) AS orders_binary,	
	COUNT(DISTINCT 
		(CASE WHEN orders.created_at > test_assignment.event_time 
		THEN invoice_id ELSE NULL
		END)) AS orders_after_assignment,
	COUNT(DISTINCT 
		(CASE WHEN orders.created_at > test_assignment.event_time 
		THEN line_item_id ELSE NULL
		END)) AS items_after_assignment,
	SUM(
		(CASE WHEN orders.created_at > test_assignment.event_time
		THEN price ELSE 0
		END)) AS total_revenue,
	test_assignment.test_assignment,
	test_assignment.test_id,
	test_assignment.user_id
  --,  test_assignment.user_id
  --DATE(orders.paid_at)
	FROM
	  (SELECT
		event_id,
		event_time,
		user_id,
		--platform,
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
			user_id
		) test_assignment
	LEFT JOIN 
	  dsv1069.orders
	ON test_assignment.user_id = orders.user_id
	GROUP BY 
	  test_assignment.test_assignment,
	  test_assignment.test_id, 
	  test_assignment.user_id
	  --DATE(paid_at)
	ORDER BY 
	  test_id,
	  test_assignment
	) mean_metrics
GROUP BY 
	test_assignment,
	test_id
ORDER BY test_id
