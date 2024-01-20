/*No. 5 Create a Rollup Table*/

/*Exercise 1: Create a subtable of orders per day. Make sure you decide whether you are counting invoices or line items.

-I counted both to see if they agree; the invoice_id agrees with the line_item_count if you dont use distinct, but there are fewer invoiced with distinct envoked. This makes sense because the invoice ID will be tracking the single purchase of multiple items, while the line_item_id tracks the items within each purchase. 
*/

SELECT 
  DATE(paid_at),
  COUNT(invoice_id) AS invoice_count,
  COUNT(line_item_id) AS line_item_count,
  COUNT(DISTINCT invoice_id) AS dist_invoice_count,
  COUNT(DISTINCT line_item_id) AS dist_line_item_count
FROM dsv1069.orders
GROUP BY DATE(paid_at);



/*Exercise 2: “Check your joins”. We are still trying to count orders per day. In this step join the sub table from the previous exercise to the dates rollup table so we can get a row for every date. Check that the join works by just running a “select *” query

Exercise 3: “Clean up your Columns” In this step be sure to specify the columns you actually want to return, and if necessary do any aggregation needed to get a count of the orders made per day.
*/

SELECT 
  DATE(dr.date),
  COUNT(DISTINCT o.invoice_id) AS invoice_count,
  COUNT(DISTINCT o.line_item_id) AS line_item_count
FROM dsv1069.dates_rollup dr
LEFT JOIN dsv1069.orders o
  ON DATE(dr.date) = DATE(o.paid_at)
GROUP BY DATE(dr.date);


/*Exercise 4: Weekly Rollup. Figure out which parts of the JOIN condition need to be edited create 7 day rolling orders table.

-Rolling average calculated by using sum() with over(); you order by date, then select rows that span 6+current row to sum over for all dates;

There's a better way to write this code as well:
SELECT *
FROM
	dsv1069.dates_rollup dr
LEFT JOIN (
	SELECT 
	  DATE(paid_at) AS day,
	  COUNT(DISTINCT invoice_id) AS dist_invoice_count,
	  COUNT(DISTINCT line_item_id) AS dist_line_item_count
	FROM dsv1069.orders
	GROUP BY DATE(paid_at);
	) daily_orders
ON dr.date = daily_orders.day

Better because you don't have to change your inner code chunk, you just make it a subquery and join to that in the outer layer; much quicker!
*/

SELECT 
  date,
  sum(invoice_count) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING 		AND CURRENT ROW) 
  AS orders_7d,
  sum(line_item_count) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING 	AND CURRENT ROW) 
  AS items_ordered_7d
FROM (
    SELECT 
      DATE(dr.date),
      COUNT(DISTINCT o.invoice_id) AS invoice_count,
      COUNT(DISTINCT o.line_item_id) AS line_item_count
    FROM dsv1069.dates_rollup dr
    LEFT JOIN dsv1069.orders o
      ON DATE(dr.date) = DATE(o.paid_at)
    GROUP BY DATE(dr.date)
    ) date_agg;
    
    
    
/* Exercise 5: Column Cleanup. Finish creating the weekly rolling orders table, by performing any aggregation steps and naming your columns appropriately.

-Done i think? Might be good to COALESCE the SUM() function, but it doesn't seem to matter.

Another way to do it; the dates_rollup table has a column named "d7_ago" which you can use to compare date ranges and sum between those dates. Seems to work by merging a buunch of rows :
*/
SELECT 
	dr.date,
	COALESCE(SUM(invoice_count),0) AS orders,
	COALESCE(SUM(line_item_count),0) AS items_ordered
FROM
	dsv1069.dates_rollup dr
LEFT JOIN (
	SELECT 
	  DATE(paid_at) AS day,
	  COUNT(DISTINCT invoice_id) AS invoice_count,
	  COUNT(DISTINCT line_item_id) AS line_item_count
	FROM 
		dsv1069.orders
	GROUP BY DATE(paid_at)
	) daily_orders
ON dr.date >= daily_orders.day
AND dr.d7_ago < daily_orders.day
GROUP BY dr.date;
 
/* 
SELECT  
  user_id,
  invoice_id,
  paid_at,
  RANK() OVER (PARTITION BY user_id ORDER BY paid_at ASC) AS order_num,
  DENSE_RANK() OVER (PARTITION BY user_id ORDER BY paid_at ASC) AS dense_order_num,
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY paid_at ASC) AS row_num
FROM 
  dsv1069.orders

RANK() is like in sports where teams with equal records have the same ranking, but the rank below their shared ranking is skipped for the team with the next worse rank (e.g., 1,2,2,4)
DENSE_RANK() doesn't care about ties and will rank thing in order without skipping a number

*/

