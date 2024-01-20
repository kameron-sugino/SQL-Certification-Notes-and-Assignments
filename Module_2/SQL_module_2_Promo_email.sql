/* No. 6 Promo Email
Exercise 1:
Create the right subtable for recently viewed events using the view_item_events table. 

-if you want the very last thing viewed, dont use DATE() since that'll trim the timestamp and return only the y/m/d format, not time
*/

SELECT
  *
FROM
  (SELECT 
    user_id,
	item_id
	event_time,
  	DENSE_RANK() OVER(PARTITION BY user_id ORDER BY event_time) 
	AS view_rank
  FROM
  	dsv1069.view_item_events
) vr
WHERE view_rank = 1
	

/*Exercise 2: Check your joins. Join your tables together recent_views, users, items 

*/

SELECT *
	FROM
		(SELECT
		  *
		FROM
		  (SELECT 
			user_id,
			item_id,
			event_time,
			DENSE_RANK() OVER(PARTITION BY user_id ORDER BY event_time) 
			AS view_rank
		  FROM
			dsv1069.view_item_events
		) vr
WHERE view_rank = 1) recent_view


/*Exercise 3: Clean up your columns. The goal of all this is to return all of the information we’ll need to send users an email about the item they viewed more recently. Clean up this query outline from the outline in EX2 and pull only the columns you need. Make sure they are named appropriately so that another human can read and understand their contents.

-I'm not super sure how to query specific tables since so many joins happened. Seems like I don't need to be that specific
*/

SELECT 
  users.id 	    	  AS user_id,
  users.email_address AS email,
  items.id	          AS item_id,
  items.name	  	  AS item_name,
  items.category      AS item_category
	FROM
		(SELECT
		  *
		FROM
		  (SELECT 
			user_id,
			item_id,
			event_time,
			DENSE_RANK() OVER(PARTITION BY user_id ORDER BY event_time) 
			AS view_rank
		  FROM
			dsv1069.view_item_events
		) vr
WHERE view_rank = 1) recent_view
JOIN
  dsv1069.users
ON COALESCE(users.parent_user_id, users.id) = recent_view.user_id
JOIN 
  dsv1069.items 
ON items.id = recent_view.item_id;




/*Exercise 4: Consider any edge cases. If we sent an email to everyone in the results of this query, what would we want to filter out. Add in any extra filtering that you think would make this email better. For example should we include deleted users? Should we send this email to users who already ordered the item they viewed most recently?

I CAN COALESCE THE IDs BY SPECIFYING IN THE SELECT() STATEMENT_ID
We filter out events before a specific time (can make it automated)
and it left joins to the orders table by user_id and item_id to find users who have already ordered the last item they viewed (left join creates nulls where the user_id and item_id don't match the order table, so we can separate out all the null values from the orders table)

I can also get rid of one of the nested queries (where I pull out the ranked_views) by specifying in the where statement at the end with the others. 
*/

SELECT 
  COALESCE(users.parent_user_id, users.id)	AS user_id,
  users.email_address 						AS email,
  items.id	         						AS item_id,
  items.name	  	 						AS item_name,
  items.category     						AS item_category,
  recent_view.event_time					AS event_time,
	FROM
		(SELECT
		  *
		FROM
		  (SELECT 
			user_id,
			item_id,
			event_time,
			DENSE_RANK() OVER(PARTITION BY user_id ORDER BY event_time) 
			AS view_rank
		  FROM
			dsv1069.view_item_events
		  WHERE
			event_time >= '2017-01-01'
		) vr
WHERE view_rank = 1) recent_view
JOIN
  dsv1069.users
ON COALESCE(users.parent_user_id, users.id) = recent_view.user_id
JOIN 
  dsv1069.items 
ON items.id = recent_view.item_id
LEFT JOIN
	dsv1069.orders
ON 
	orders.item_id = recent_view.item_id 
AND 
	orders.user_id = recent_view.user_id
WHERE users.deleted_at IS NULL
AND orders.item_id IS NULL;
