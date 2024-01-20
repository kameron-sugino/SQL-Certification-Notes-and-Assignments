/*No. 2 Flexible data formats*/
SELECT event_id,
event_time,
user_id,
platform,
/* #these two chunks pull specific row values, but leaves NULLs behind. To aggregte them together, you need to group by all the other rows (see last line). Basically pivots the table. */
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
	
	
/* #checking data for reliability
GROUP BY to check all values expected actually exist
-can also exclude NULLs to pare down the data
 */