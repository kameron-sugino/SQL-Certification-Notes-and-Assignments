/* No. 8 Product Analysis

-I don't really like how they handled this. There weren't any data that would convince me to implement a change in behavior; at least, I didn't see anything convincing. 
*/

/* Exercise 0: Count how many users we have 

-I think we want to remove any accounts that were deleted, and merge the parent and user ID's together, then only count distinct entries to get a clean total of unique, active accounts.

There are 111235 users that fit this description
*/
SELECT 
  COUNT(DISTINCT COALESCE(parent_user_id, id))
FROM 
  dsv1069.users
WHERE deleted_at IS NULL;


/* Exercise 1: Find out how many users have ever ordered 

-total numer of distinct user_id in orders is 17463. If we inner join users to orders, itll filter any users who haven't ordered, and then we can COALESCE on parent_user_id and user_id again to get the true unique accounts that have ordered, which is 16904; using users.id instead of orders.user_id gives the same answer
*/
SELECT 
  COUNT(DISTINCT user_id)
FROM dsv1069.orders


SELECT 
  COUNT(DISTINCT COALESCE(users.parent_user_id, orders.user_id))
FROM 
  dsv1069.users
JOIN 
  dsv1069.orders
ON users.id = orders.user_id
WHERE users.deleted_at IS NULL;


/* Exercise 2: Goal find how many users have reordered the same item 

Here I created two subqueries of the same table, and joined them together where one table handled the join of the first order, and the other table handled the join of any reorders of the same item by the same user (but at a different date). This came out with only 4 instances of a user reordering the same item at a different date, as opposed to users ordering multiple of the same item at one time.

Since I was having a weird time getting my code for Ex.7 to work, I retried it without the removal of the deleted users and merger of the parent/child ID and the result is the same

CORRECT SOLUTION:
I don't really understand why grouping the distinct counts of the line_item_id by item_id and user is indicative of reorders, but I also don't really understand what the line_item_id value is...

SELECT
  COUNT(*),
  COUNT(DISTINCT line_item_id)
FROM
  dsv1069.orders
	
If we look at the code here, it shows that the count of rows is equivalent to the count of distinct line_item_id (47420), so I think what they're really counting is how many times a person purchase an item regardless of time (i.e., they likely purchased multiple of the same item at one time, not at different times)	
*/

SELECT
	COUNT(DISTINCT user_id) AS users_who_reordered
FROM
	(SELECT
		user_id,
		item_id,
		COUNT(DISTINCT line_item_id) AS times_user_ordered
	FROM
		dsv1069.orders
	GROUP BY
		user_id,
		item_id	
	) user_level_orders
WHERE times_user_ordered > 1
	

SELECT 
  orders_1.order_num,
  orders_1.user_id,
  orders_1.item_id,
  orders_1.paid_at,
  orders_2.order_num,
  orders_2.user_id,
  orders_2.item_id,
  orders_2.paid_at
FROM
	(SELECT 
	  DENSE_RANK() OVER (PARTITION BY user_id ORDER BY paid_at) AS order_num,
	  COALESCE(users.parent_user_id, users.id) AS user_id,
	  orders.item_id,
	  orders.item_name,
	  orders.item_category,
	  orders.paid_at
	FROM 
	  dsv1069.users
	JOIN 
	  dsv1069.orders
	ON users.id = orders.user_id
	WHERE users.deleted_at IS NULL) orders_1
JOIN 
	(SELECT 
	  DENSE_RANK() OVER (PARTITION BY user_id ORDER BY paid_at) AS order_num,
	  COALESCE(users.parent_user_id, users.id) AS user_id,
	  orders.item_id,
	  orders.item_name,
	  orders.item_category,
	  orders.paid_at
	FROM 
	  dsv1069.users
	JOIN 
	  dsv1069.orders
	ON users.id = orders.user_id
	WHERE users.deleted_at IS NULL) orders_2
ON 
  orders_1.user_id = orders_2.user_id 
AND orders_1.item_id = orders_2.item_id 
WHERE orders_1.order_num = 1
AND orders_2.order_num > 1




/* Exercise 3: Do users even order more than once? 

-There are 3679 instances of users who ordered more than once, and 1379 users who ordered more than once


CORRECT SOLUTION:

SELECT 
  COUNT(DISTINCT user_id)
FROM
  (SELECT
  	user_id,
  	COUNT(DISTINCT invoice_id) AS invoice
  FROM 
  	dsv1069.orders
  GROUP BY user_id) invoice_count
WHERE invoice > 1

1421

-I was double counting invoices, so my first number is much higher. My second number may be due to my trimming of the dataset, but I have no proof of that (and I need to move on). I kinda forgot that the invoice column existed. Whoops
*/
SELECT 
  COUNT(order_num)
FROM
  (SELECT 
	  DENSE_RANK() OVER (PARTITION BY user_id ORDER BY paid_at) AS order_num,
	  COALESCE(users.parent_user_id, users.id) AS user_id
  FROM 
    dsv1069.users
  JOIN 
    dsv1069.orders
  ON users.id = orders.user_id
  WHERE users.deleted_at IS NULL) orders
WHERE order_num > 1

SELECT 
  COUNT(DISTINCT user_id)
FROM
  (SELECT 
	  DENSE_RANK() OVER (PARTITION BY user_id ORDER BY paid_at) AS order_num,
	  COALESCE(users.parent_user_id, users.id) AS user_id
  FROM 
    dsv1069.users
  JOIN 
    dsv1069.orders
  ON users.id = orders.user_id
  WHERE users.deleted_at IS NULL) orders
WHERE order_num > 1



/* Exercise 4: Orders per item 

-max is 40 by item_id, min non-zero value is 9
They count by line_item_id, which, again, is the same as just counting rows (they got the same result as me)
*/

SELECT 
  COUNT(item_id),
  item_id
FROM 
  dsv1069.orders
GROUP BY 
  item_id
ORDER BY COUNT(item_id) DESC

/* Exercise 5: Orders per category 

-ranges from 4892-4633 orders per category. there are 10 categories total
*/

SELECT 
  COUNT(item_category),
  item_category
FROM 
  dsv1069.orders
GROUP BY 
  item_category
ORDER BY COUNT(item_category) DESC




/* Exercise 6: Goal: Do user order multiple things from the same category? 

-There were 921 instances of users reordering from the same category. This method double counts some items because the categories will match to multiple if they exist. If we count only distinct line_item_ids then we get ~387/386 instances of reordering the same category (depending on if we run distinct on order 1 or order 2). If we just want the number of things ordered by the same category regardless of purchase date, then there are 13662 instances (out of 16904 users who have ever ordered) that order items from the same category.
*/

SELECT 
  COUNT(*)
FROM
	(SELECT 
	  DENSE_RANK() OVER (PARTITION BY user_id ORDER BY paid_at) AS order_num,
	  COALESCE(users.parent_user_id, users.id) AS user_id,
	  orders.item_id,
	  orders.item_name,
	  orders.item_category,
	  orders.paid_at
	FROM 
	  dsv1069.users
	JOIN 
	  dsv1069.orders
	ON users.id = orders.user_id
	WHERE users.deleted_at IS NULL) orders_1
JOIN 
	(SELECT 
	  DENSE_RANK() OVER (PARTITION BY user_id ORDER BY paid_at) AS order_num,
	  COALESCE(users.parent_user_id, users.id) AS user_id,
	  orders.item_id,
	  orders.item_name,
	  orders.item_category,
	  orders.paid_at
	FROM 
	  dsv1069.users
	JOIN 
	  dsv1069.orders
	ON users.id = orders.user_id
	WHERE users.deleted_at IS NULL) orders_2
ON 
  orders_1.user_id = orders_2.user_id 
AND orders_1.item_category = orders_2.item_category 
WHERE orders_1.order_num = 1
AND orders_2.order_num > 1

####

SELECT 
	  COUNT(DISTINCT orders_1.line_item_id),
	  COUNT(DISTINCT orders_2.line_item_id)
FROM
	(SELECT 
	  DENSE_RANK() OVER (PARTITION BY user_id ORDER BY paid_at) AS order_num,
	  COALESCE(users.parent_user_id, users.id) AS user_id,
	  orders.item_id,
	  orders.line_item_id,
	  orders.item_name,
	  orders.item_category,
	  orders.paid_at
	FROM 
	  dsv1069.users
	JOIN 
	  dsv1069.orders
	ON users.id = orders.user_id
	WHERE users.deleted_at IS NULL) orders_1
JOIN 
	(SELECT 
	  DENSE_RANK() OVER (PARTITION BY user_id ORDER BY paid_at) AS order_num,
	  COALESCE(users.parent_user_id, users.id) AS user_id,
	  orders.item_id,
	  orders.line_item_id,
	  orders.item_name,
	  orders.item_category,
	  orders.paid_at
	FROM 
	  dsv1069.users
	JOIN 
	  dsv1069.orders
	ON users.id = orders.user_id
	WHERE users.deleted_at IS NULL) orders_2
ON 
  orders_1.user_id = orders_2.user_id 
AND orders_1.item_category = orders_2.item_category 
WHERE orders_1.order_num = 1
AND orders_2.order_num > 1

####

SELECT 
  AVG(*)
FROM
  (SELECT 
    COUNT(item_category) AS item_count,
    item_category,
    user_id
  FROM 
    dsv1069.orders
  GROUP BY 
    item_category,
    user_id) item_count_by_category_and_user
WHERE item_count > 1

###
SELECT 
  AVG(item_count),
  item_category
FROM
  (SELECT 
    COUNT(item_category) AS item_count,
    item_category,
    user_id
  FROM 
    dsv1069.orders
  GROUP BY 
    item_category,
    user_id) item_count_by_category_and_user
WHERE item_count > 1
GROUP BY item_category


/* Exercise 7: Goal: Find the average time between orders
--Decide if this analysis is necessary 


-for some reason, when I was trying to incorporate removing deleted users, and merging the parent and child user_ids it kept giving my large negative date_diff values (even though the dates did not exist previously when I looked at each nested subquery) so I used the given code to calculate that the min time for reorders is 0 days, avg is 66.05 days, and the max is 197 days. 
*/

SELECT  
  MIN(date_diff),
  AVG(date_diff),
  MAX(date_diff)
FROM
  (SELECT 
    DISTINCT *
  FROM
    (SELECT 
      orders_1.user_id,
      DATE(orders_1.paid_at) AS first_order,
      DATE(orders_2.paid_at) AS second_order,
      DATE(orders_2.paid_at) - DATE(orders_1.paid_at) AS date_diff
    FROM
    (SELECT 
    	  DENSE_RANK() OVER (PARTITION BY user_id ORDER BY paid_at ASC) AS order_num,
    	  orders.user_id AS user_id,
    	  orders.invoice_id,
    	  orders.paid_at
    	FROM 
    	  dsv1069.orders) orders_1
    JOIN 
    	(SELECT 
    	  DENSE_RANK() OVER (PARTITION BY user_id ORDER BY paid_at ASC) AS order_num,
    	  orders.user_id AS user_id,
    	  orders.invoice_id,
    	  orders.paid_at
    	FROM 
    	  dsv1069.orders) orders_2
    ON 
      orders_1.user_id = orders_2.user_id 
    WHERE orders_1.order_num = 1
    AND orders_2.order_num = 2) order_diff) order_diff_distinct
