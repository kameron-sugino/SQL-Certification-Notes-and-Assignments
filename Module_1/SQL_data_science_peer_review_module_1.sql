/* Data Scientist Role Play: Profiling and Analyzing the Yelp Dataset Coursera Worksheet

This is a 2-part assignment. In the first part, you are asked a series of questions that will help you profile and understand the data just like a data scientist would. For this first part of the assignment, you will be assessed both on the correctness of your findings, as well as the code you used to arrive at your answer. You will be graded on how easy your code is to read, so remember to use proper formatting and comments where necessary.

In the second part of the assignment, you are asked to come up with your own inferences and analysis of the data for a particular research question you want to answer. You will be required to prepare the dataset for the analysis you choose to do. As with the first part, you will be graded, in part, on how easy your code is to read, so use proper formatting and comments to illustrate and communicate your intent as required.

For both parts of this assignment, use this "worksheet." It provides all the questions you are being asked, and your job will be to transfer your answers and SQL coding where indicated into this worksheet so that your peers can review your work. You should be able to use any Text Editor (Windows Notepad, Apple TextEdit, Notepad ++, Sublime Text, etc.) to copy and paste your answers. If you are going to use Word or some other page layout application, just be careful to make sure your answers and code are lined appropriately.
In this case, you may want to save as a PDF to ensure your formatting remains intact for you reviewer.
 */


/* Part 1: Yelp Dataset Profiling and Understanding

1. Profile the data by finding the total number of records for each of the tables below:
	 */
/*this is by far the dumbest way I've had to compile row counts...*/

SELECT COUNT(1)
FROM Attribute
UNION ALL

SELECT COUNT(1)
FROM Business
UNION ALL

SELECT COUNT(1)
FROM Category
UNION ALL

SELECT COUNT(1)
FROM Checkin
UNION ALL

SELECT COUNT(1)
FROM elite_years
UNION ALL

SELECT COUNT(1)
FROM friend
UNION ALL

SELECT COUNT(1)
FROM hours
UNION ALL

SELECT COUNT(1)
FROM photo
UNION ALL

SELECT COUNT(1)
FROM review
UNION ALL

SELECT COUNT(1)
FROM tip
UNION ALL

SELECT COUNT(*)
FROM user;



/*output: all tables had 10,000 rows
+----------+
| COUNT(1) |
+----------+
|    10000 |
|    10000 |
|    10000 |
|    10000 |
|    10000 |
|    10000 |
|    10000 |
|    10000 |
|    10000 |
|    10000 |
|    10000 |
+----------+

i. Attribute table = 10000
ii. Business table = 10000
iii. Category table = 10000
iv. Checkin table = 10000
v. elite_years table = 10000
vi. friend table =  10000
vii. hours table = 10000
viii. photo table =  10000
ix. review table =  10000
x. tip table =  10000
xi. user table = 10000
	


2. Find the total distinct records by either the foreign key or primary key for each table. If two foreign keys are listed in the table, please specify which foreign key.

/*primary key*/
SELECT COUNT(DISTINCT(id))
FROM business;
+---------------------+
| COUNT(DISTINCT(id)) |
+---------------------+
|               10000 |
+---------------------+

/*foreign key*/
SELECT COUNT(DISTINCT(business_id))
FROM hours;
+------------------------------+
| COUNT(DISTINCT(business_id)) |
+------------------------------+
|                         1562 |
+------------------------------+

/*foreign key*/
SELECT COUNT(DISTINCT(business_id))
FROM category;
+------------------------------+
| COUNT(DISTINCT(business_id)) |
+------------------------------+
|                         2643 |
+------------------------------+

/*foreign key*/
SELECT COUNT(DISTINCT(business_id))
FROM attribute;
+------------------------------+
| COUNT(DISTINCT(business_id)) |
+------------------------------+
|                         1115 |
+------------------------------+

/*primary key*/
SELECT COUNT(DISTINCT(id))
FROM review
UNION ALL
/*foreign key*/
SELECT COUNT(DISTINCT(business_id))
FROM review
UNION ALL
/*foreign key*/
SELECT COUNT(DISTINCT(user_id))
FROM review;
+---------------------+
| COUNT(DISTINCT(id)) |
+---------------------+
|               10000 |
|                8090 |
|                9581 |
+---------------------+

/*foreign key*/
SELECT COUNT(DISTINCT(business_id))
FROM checkin;
+------------------------------+
| COUNT(DISTINCT(business_id)) |
+------------------------------+
|                          493 |
+------------------------------+

/*primary key*/
SELECT COUNT(DISTINCT(id))
FROM photo
UNION ALL
/*foreign key*/
SELECT COUNT(DISTINCT(business_id))
FROM photo;
+---------------------+
| COUNT(DISTINCT(id)) |
+---------------------+
|               10000 |
|                6493 |
+---------------------+

/*foreign key*/
SELECT COUNT(DISTINCT(user_id))
FROM tip
UNION ALL
/*foreign key*/
SELECT COUNT(DISTINCT(business_id))
FROM tip;
+--------------------------+
| COUNT(DISTINCT(user_id)) |
+--------------------------+
|                      537 |
|                     3979 |
+--------------------------+

/*primary key*/
SELECT COUNT(DISTINCT(id))
FROM user;
+---------------------+
| COUNT(DISTINCT(id)) |
+---------------------+
|               10000 |
+---------------------+

/*foreign key*/
SELECT COUNT(DISTINCT(user_id))
FROM friend;
+--------------------------+
| COUNT(DISTINCT(user_id)) |
+--------------------------+
|                       11 |
+--------------------------+

/*foreign key*/
SELECT COUNT(DISTINCT(user_id))
FROM elite_years;
+--------------------------+
| COUNT(DISTINCT(user_id)) |
+--------------------------+
|                     2780 |
+--------------------------+

/* i. Business = 10000
ii. Hours = 1562
iii. Category = 2643
iv. Attribute = 1115
v. Review = 10000; business_id: 8090; user_id: 9581
vi. Checkin = 493
vii. Photo = 10000; 6493
viii. Tip = 537; 3979 
ix. User = 10000
x. Friend = 11
xi. Elite_years = 2780

Note: Primary Keys are denoted in the ER-Diagram with a yellow key icon.	
 */

/* 3. Are there any columns with null values in the Users table? Indicate "yes," or "no."

	Answer: no
	
	
	SQL code used to arrive at answer:
	 */
SELECT
	COUNT(*)-COUNT(1) AS A,
	COUNT(*)-COUNT(2) AS B,
	COUNT(*)-COUNT(3) AS C,
	COUNT(*)-COUNT(4) AS D,
	COUNT(*)-COUNT(5) AS E,
	COUNT(*)-COUNT(6) AS F,
	COUNT(*)-COUNT(7) AS G,
	COUNT(*)-COUNT(8) AS H,
	COUNT(*)-COUNT(9) AS I,
	COUNT(*)-COUNT(10) AS J,
	COUNT(*)-COUNT(11) AS K,
	COUNT(*)-COUNT(12) AS L,
	COUNT(*)-COUNT(13) AS M,
	COUNT(*)-COUNT(14) AS N,
	COUNT(*)-COUNT(15) AS O,
	COUNT(*)-COUNT(16) AS P,
	COUNT(*)-COUNT(17) AS Q,
	COUNT(*)-COUNT(18) AS R,
	COUNT(*)-COUNT(19) AS S,
	COUNT(*)-COUNT(20) AS T
FROM user; 

/*alternative answer uses IS NULL to sum all the null values across all rows. However, this answer only give you the total number of NULL values and not the columns where they exist:

SELECT COUNT(*) 
FROM user 
WHERE 1 IS NULL OR 2 IS NULL OR 3 IS NULL OR 4 IS NULL OR 5 IS NULL OR 6 IS NULL OR 7 IS NULL OR 8 IS NULL
	OR 9 IS NULL OR 10 IS NULL OR 11 IS NULL OR 12 IS NULL OR 13 IS NULL OR 14 IS NULL OR 15 IS NULL
	OR 16 IS NULL OR 17 IS NULL OR 18 IS NULL OR 19 IS NULL OR 20 IS NULL;
*/

	
/* 4. For each table and column listed below, display the smallest (minimum), largest (maximum), and average (mean) value for the following fields:
 */
SELECT MIN(stars),
MAX(stars),
AVG(stars)
FROM review;

SELECT MIN(stars),
MAX(stars),
AVG(stars)
FROM business;

SELECT MIN(likes),
MAX(likes),
AVG(likes)
FROM tip;

SELECT MIN(count),
MAX(count),
AVG(count)
FROM checkin;

SELECT MIN(review_count),
MAX(review_count),
AVG(review_count)
FROM user;

/* 	i. Table: Review, Column: Stars
	
		min: 1		max: 5		avg: 3.7082
		
	
	ii. Table: Business, Column: Stars
	
		min: 1		max: 5		avg: 3.6549
		
	
	iii. Table: Tip, Column: Likes
	
		min: 0		max: 2		avg: 0.0144
		
	
	iv. Table: Checkin, Column: Count
	
		min: 1		max: 53		avg: 1.9414
		
	
	v. Table: User, Column: Review_count
	
		min: 0		max: 2000		avg: 24.2995
		
 */
 
/* 
5. List the cities with the most reviews in descending order:

	SQL code used to arrive at answer:
	 */
	SELECT city,
	COUNT(review_count)
	FROM business
	GROUP BY city
	ORDER BY COUNT(review_count) DESC;
	
/* 	Copy and Paste the Result Below:
	+-----------------+---------------------+
	| city            | COUNT(review_count) |
	+-----------------+---------------------+
	| Las Vegas       |                1561 |
	| Phoenix         |                1001 |
	| Toronto         |                 985 |
	| Scottsdale      |                 497 |
	| Charlotte       |                 468 |
	| Pittsburgh      |                 353 |
	| Montréal        |                 337 |
	| Mesa            |                 304 |
	| Henderson       |                 274 |
	| Tempe           |                 261 |
	| Edinburgh       |                 239 |
	| Chandler        |                 232 |
	| Cleveland       |                 189 |
	| Gilbert         |                 188 |
	| Glendale        |                 188 |
	| Madison         |                 176 |
	| Mississauga     |                 150 |
	| Stuttgart       |                 141 |
	| Peoria          |                 105 |
	| Markham         |                  80 |
	| Champaign       |                  71 |
	| North Las Vegas |                  70 |
	| North York      |                  64 |
	| Surprise        |                  60 |
	| Richmond Hill   |                  54 |
	+-----------------+---------------------+
	(Output limit exceeded, 25 of 362 total rows shown)
 */
	
/* 6. Find the distribution of star ratings to the business in the following cities:

i. Avon

SQL code used to arrive at answer:
 */
	SELECT stars,
	COUNT(stars)
	FROM business
	WHERE city = "Avon"
	GROUP BY stars
	ORDER BY stars ASC;

/* Copy and Paste the Resulting Table Below (2 columns â€“ star rating and count):

	+-------+--------------+
	| stars | COUNT(stars) |
	+-------+--------------+
	|   1.5 |            1 |
	|   2.5 |            2 |
	|   3.5 |            3 |
	|   4.0 |            2 |
	|   4.5 |            1 |
	|   5.0 |            1 |
	+-------+--------------+
 */
 
/* ii. Beachwood

SQL code used to arrive at answer:
 */
	SELECT stars,
	COUNT(stars)
	FROM business
	WHERE city = "Beachwood"
	GROUP BY stars
	ORDER BY stars ASC;

/* Copy and Paste the Resulting Table Below (2 columns â€“ star rating and count):

	+-------+--------------+
	| stars | COUNT(stars) |
	+-------+--------------+
	|   2.0 |            1 |
	|   2.5 |            1 |
	|   3.0 |            2 |
	|   3.5 |            2 |
	|   4.0 |            1 |
	|   4.5 |            2 |
	|   5.0 |            5 |
	+-------+--------------+
 */

/* 7. Find the top 3 users based on their total number of reviews:
		
	SQL code used to arrive at answer:
	 */
	SELECT name,
	review_count
	FROM user
	ORDER BY review_count DESC
	LIMIT 3;	
	
/* 	Copy and Paste the Result Below:
	
	+-----------+--------------+
	| name      | review_count |
	+-----------+--------------+
	| Gerald    |         2000 |
	| Sara      |         1629 |
	| Yuri      |         1339 |
	+-----------+--------------+
	(Output limit exceeded, 3 of 10000 total rows shown)
 */
/* 8. Does posing more reviews correlate with more fans?

	Please explain your findings and interpretation of the results:
	 */
	 
/*this is horrible. I tried this 2 ways, calculating the slope from min->avg, avg->max, and min->max; This is a bad way of looking at the data so I'm igorning it. I also refactored review_count and fans into greater or less than average, then grouped by score to get back counts for each of the 4 possible outcomes. This gives back a density curve of sorts. Looking at the top of the table:	
	+---------------+---------------+----------+
	| review_score  | fans_score    | COUNT(*) |
	+---------------+---------------+----------+
	| above average | above average |      986 |
	| above average | below average |      805 |
	
	Shows us that having an above average number of reviews does not associate with the number of fans.
	There's no shot I'm going to write code to make a full regression line; sql isn't built for that.
	
	Code:
*/
		SELECT 
	CASE WHEN review_count >= 24.2995 
		THEN 'above average'
		ELSE 'below average'
	END AS review_score,
    CASE WHEN fans > 1.4896
       THEN 'above average'
       ELSE 'below average'
    END AS fans_score,
    COUNT(*)
	FROM user
    GROUP BY review_score, fans_score;

/* 	+---------------+---------------+----------+
	| review_score  | fans_score    | COUNT(*) |
	+---------------+---------------+----------+
	| above average | above average |      986 |
	| above average | below average |      805 |
	| below average | above average |      223 |
	| below average | below average |     7986 |
	+---------------+---------------+----------+
	 */
	
	
	SELECT MIN(review_count),
    AVG(review_count),
    MAX(review_count),
    MIN(fans),
	AVG(fans),
    MAX(fans)
	FROM user;	
/* 	+-------------------+-------------------+-------------------+-----------+-----------+-----------+
	| MIN(review_count) | AVG(review_count) | MAX(review_count) | MIN(fans) | AVG(fans) | MAX(fans) |
	+-------------------+-------------------+-------------------+-----------+-----------+-----------+
	|                 0 |           24.2995 |              2000 |         0 |    1.4896 |       503 |
	+-------------------+-------------------+-------------------+-----------+-----------+-----------+
	 */
	 
/* we can also calculate slope with (y1-y2)/(x1-x2); we'll look at min to average, avg to max, and min to max slopes
*/
	 
	SELECT (AVG(review_count)-MIN(review_count))/(
    AVG(fans)-MIN(fans))
	FROM user;
	min->avg: 16.31
	
	
	SELECT (MAX(review_count)-AVG(review_count))/(
    MAX(fans)-AVG(fans))
	FROM user;
	avg->max: 3.9
	
	SELECT (MAX(review_count)-MIN(review_count))/(
    MAX(fans)-MIN(fans))
	FROM user;
	min->max: 3.0

	
	
	
/* 9. Are there more reviews with the word "love" or with the word "hate" in them?

	Answer:
	
	More reviews with love by a factor of ~8
	
	SQL code used to arrive at answer:
	 */
	SELECT COUNT(*)
	FROM review
	WHERE text LIKE "%love%";
	
/* 	+----------+
	| COUNT(*) |
	+----------+
	|     1780 |
	+----------+
	 */
	SELECT COUNT(*)
	FROM review
	WHERE text LIKE "%hate%";
/* 	+----------+
	| COUNT(*) |
	+----------+
	|      232 |
	+----------+
	 */

/* 10. Find the top 10 users with the most fans:

	SQL code used to arrive at answer:
	 */
	SELECT name,
	fans
	FROM user
	ORDER BY fans DESC
	LIMIT 10;
	
/* 	Copy and Paste the Result Below:

	+-----------+------+
	| name      | fans |
	+-----------+------+
	| Amy       |  503 |
	| Mimi      |  497 |
	| Harald    |  311 |
	| Gerald    |  253 |
	| Christine |  173 |
	| Lisa      |  159 |
	| Cat       |  133 |
	| William   |  126 |
	| Fran      |  124 |
	| Lissa     |  120 |
	+-----------+------+
		 */

/* Part 2: Inferences and Analysis

1. Pick one city and category of your choice and group the businesses in that city or category by their overall star rating. Compare the businesses with 2-3 stars to the businesses with 4-5 stars and answer the following questions. Include your code.
	 */
	/* query my choices */

	SELECT b.city,
	c.category,
	COUNT(c.category)
	FROM business b
	JOIN category c ON b.id = c.business_id
	GROUP BY b.city, c.category
	ORDER BY COUNT(c.category) DESC;
	
/* 	+-------------+------------------------+-------------------+
	| city        | category               | COUNT(c.category) |
	+-------------+------------------------+-------------------+
	| Toronto     | Restaurants            |                10 |
	| Phoenix     | Restaurants            |                 6 |
	| Chandler    | Shopping               |                 4 |
	| Las Vegas   | Health & Medical       |                 4 |
	| Las Vegas   | Restaurants            |                 4 |
	| Las Vegas   | Shopping               |                 4 |
	| Mississauga | Restaurants            |                 4 |
	| Phoenix     | Food                   |                 4 |
	| Phoenix     | Home Services          |                 4 |
	| Toronto     | Bars                   |                 4 |
	| Toronto     | Food                   |                 4 |
	| Toronto     | Nightlife              |                 4 |
	| Toronto     | Shopping               |                 4 |
	| Charlotte   | Shopping               |                 3 |
	| Cleveland   | Food                   |                 3 |
	| Edinburgh   | Restaurants            |                 3 |
	| Phoenix     | American (Traditional) |                 3 |
	| Phoenix     | Health & Medical       |                 3 |
	| Toronto     | Active Life            |                 3 |
	| Toronto     | Beauty & Spas          |                 3 |
	| Toronto     | Japanese               |                 3 |
	| Toronto     | Pubs                   |                 3 |
	| Toronto     | Sushi Bars             |                 3 |
	| Chandler    | Auto Repair            |                 2 |
	| Chandler    | Automotive             |                 2 |
	+-------------+------------------------+-------------------+
	(Output limit exceeded, 25 of 586 total rows shown)
		
	city = Toronto
	category = Restaurants */
	
	/* recategorize the restuarant ratings, pare data down to my city and category, and remove all low rating restaurants (there are none)*/
	
	SELECT *,
		CASE
			WHEN b.stars >=4
				THEN "High rating"
			WHEN b.stars >=2 & b.stars <= 3
				THEN "Mid rating"
			ELSE "Low rating"
		END AS rating
	FROM business b
		JOIN category c ON b.id = c.business_id
	WHERE rating != "Low rating" AND b.city="Toronto" AND c.category="Restaurants";


	
/* i. Do the two groups you chose to analyze have a different distribution of hours?
 */
	/* So, I have no idea how to create density plots and such in sql (and it's looking like that's not advisable anyway). So, I've broken down the code into days of the week to sort through by hand. I'm not even sure how to parse the date into day-of-week and time with the STRFTIME function since ti doesnt acces regexes like % to match the "|".
	*/
	SELECT 
	hours,
		CASE
			WHEN b.stars >=4
				THEN "High rating"
			WHEN b.stars >=2 & b.stars <= 3
				THEN "Mid rating"
			ELSE "Low rating"
		END AS rating
	FROM business b
		JOIN category c ON b.id = c.business_id
		JOIN hours h ON h.business_id = c.business_id
	WHERE rating != "Low rating" AND b.city="Toronto" AND c.category="Restaurants"
	AND hours LIKE "Monday%"
	GROUP BY rating, hours;
/* 
	+--------------------+-------------+
	| hours              | rating      |
	+--------------------+-------------+
	| Monday|11:00-23:00 | High rating |
	| Monday|16:00-2:00  | High rating |
	| Monday|10:30-21:00 | Mid rating  |
	| Monday|11:00-23:00 | Mid rating  |
	| Monday|9:00-23:00  | Mid rating  |
	+--------------------+-------------+ */
	

	SELECT 
	hours,
		CASE
			WHEN b.stars >=4
				THEN "High rating"
			WHEN b.stars >=2 & b.stars <= 3
				THEN "Mid rating"
			ELSE "Low rating"
		END AS rating
	FROM business b
		JOIN category c ON b.id = c.business_id
		JOIN hours h ON h.business_id = c.business_id
	WHERE rating != "Low rating" AND b.city="Toronto" AND c.category="Restaurants"
	AND hours LIKE "Tuesday%"
	GROUP BY rating, hours;

/* 	+---------------------+-------------+
	| hours               | rating      |
	+---------------------+-------------+
	| Tuesday|11:00-23:00 | High rating |
	| Tuesday|18:00-2:00  | High rating |
	| Tuesday|10:30-21:00 | Mid rating  |
	| Tuesday|11:00-23:00 | Mid rating  |
	| Tuesday|9:00-23:00  | Mid rating  |
	+---------------------+-------------+ */


	SELECT 
	hours,
		CASE
			WHEN b.stars >=4
				THEN "High rating"
			WHEN b.stars >=2 & b.stars <= 3
				THEN "Mid rating"
			ELSE "Low rating"
		END AS rating
	FROM business b
		JOIN category c ON b.id = c.business_id
		JOIN hours h ON h.business_id = c.business_id
	WHERE rating != "Low rating" AND b.city="Toronto" AND c.category="Restaurants"
	AND hours LIKE "Wednesday%"
	GROUP BY rating, hours;
	
/* 	+-----------------------+-------------+
	| hours                 | rating      |
	+-----------------------+-------------+
	| Wednesday|11:00-23:00 | High rating |
	| Wednesday|18:00-23:00 | High rating |
	| Wednesday|18:00-2:00  | High rating |
	| Wednesday|10:30-21:00 | Mid rating  |
	| Wednesday|11:00-23:00 | Mid rating  |
	| Wednesday|9:00-23:00  | Mid rating  |
	+-----------------------+-------------+ */
	

	SELECT 
	hours,
		CASE
			WHEN b.stars >=4
				THEN "High rating"
			WHEN b.stars >=2 & b.stars <= 3
				THEN "Mid rating"
			ELSE "Low rating"
		END AS rating
	FROM business b
		JOIN category c ON b.id = c.business_id
		JOIN hours h ON h.business_id = c.business_id
	WHERE rating != "Low rating" AND b.city="Toronto" AND c.category="Restaurants"
	AND hours LIKE "Thursday%"
	GROUP BY rating, hours;
/* 
	+----------------------+-------------+
	| hours                | rating      |
	+----------------------+-------------+
	| Thursday|11:00-23:00 | High rating |
	| Thursday|18:00-23:00 | High rating |
	| Thursday|18:00-2:00  | High rating |
	| Thursday|10:30-21:00 | Mid rating  |
	| Thursday|11:00-23:00 | Mid rating  |
	| Thursday|9:00-23:00  | Mid rating  |
	+----------------------+-------------+ */


	SELECT 
	hours,
		CASE
			WHEN b.stars >=4
				THEN "High rating"
			WHEN b.stars >=2 & b.stars <= 3
				THEN "Mid rating"
			ELSE "Low rating"
		END AS rating
	FROM business b
		JOIN category c ON b.id = c.business_id
		JOIN hours h ON h.business_id = c.business_id
	WHERE rating != "Low rating" AND b.city="Toronto" AND c.category="Restaurants"
	AND hours LIKE "Friday%"
	GROUP BY rating, hours;
	
/* 	+--------------------+-------------+
	| hours              | rating      |
	+--------------------+-------------+
	| Friday|11:00-23:00 | High rating |
	| Friday|18:00-23:00 | High rating |
	| Friday|18:00-2:00  | High rating |
	| Friday|10:30-21:00 | Mid rating  |
	| Friday|11:00-23:00 | Mid rating  |
	| Friday|9:00-4:00   | Mid rating  |
	+--------------------+-------------+ */
	
	
	SELECT 
	hours,
		CASE
			WHEN b.stars >=4
				THEN "High rating"
			WHEN b.stars >=2 & b.stars <= 3
				THEN "Mid rating"
			ELSE "Low rating"
		END AS rating
	FROM business b
		JOIN category c ON b.id = c.business_id
		JOIN hours h ON h.business_id = c.business_id
	WHERE rating != "Low rating" AND b.city="Toronto" AND c.category="Restaurants"
	AND hours LIKE "Saturday%"
	GROUP BY rating, hours;
	
/* 	+----------------------+-------------+
	| hours                | rating      |
	+----------------------+-------------+
	| Saturday|11:00-23:00 | High rating |
	| Saturday|16:00-2:00  | High rating |
	| Saturday|18:00-23:00 | High rating |
	| Saturday|10:00-4:00  | Mid rating  |
	| Saturday|10:30-21:00 | Mid rating  |
	| Saturday|11:00-23:00 | Mid rating  |
	+----------------------+-------------+
	 */
	SELECT 
	hours,
		CASE
			WHEN b.stars >=4
				THEN "High rating"
			WHEN b.stars >=2 & b.stars <= 3
				THEN "Mid rating"
			ELSE "Low rating"
		END AS rating
	FROM business b
		JOIN category c ON b.id = c.business_id
		JOIN hours h ON h.business_id = c.business_id
	WHERE rating != "Low rating" AND b.city="Toronto" AND c.category="Restaurants"
	AND hours LIKE "Sunday%"
	GROUP BY rating, hours;
	
/* 	+--------------------+-------------+
	| hours              | rating      |
	+--------------------+-------------+
	| Sunday|12:00-16:00 | High rating |
	| Sunday|14:00-23:00 | High rating |
	| Sunday|16:00-2:00  | High rating |
	| Sunday|10:00-23:00 | Mid rating  |
	| Sunday|11:00-19:00 | Mid rating  |
	| Sunday|11:00-23:00 | Mid rating  |
	+--------------------+-------------+ */
	
	/* CONCLUSIONS:
	
		Note: do I for sure know which restuarants are changin their opening hours each day? no, because I didn't track that in my output and I'm not re-doing this whole list again. but I'm assuming that when the other hours all stay stable, that the one changing from day-to-day is the same business.
	
		Monday: Generally open from 11:00-23:00
		Tuesday: One restaurant looks like it opens 2 hours later on Tuesday, otherwise same as monday
		Wednesday: An additional high rating restaurant is open today, otherwise same as Tuesday
		Thursday: Same as Wednesday
		Friday: One mid-rating restaurant is open 5 hours later, until 4 am, otherwise the same	
		Saturday: the restaurant that opened 2 hours later on Tuesday is openeing 2 hours earlier today and the resturant that was open until 4 am opens 1 hour later, otherwise same as Friday
		Sunday: Looks like high rating restaurant shortens their hours to only be open for 4 hrs, while another is open longer. The mid rating restuarants show a similar pattern.
	*/
	
/* ii. Do the two groups you chose to analyze have a different number of reviews?
 */
	/* Yes, the higher rated restaurants have many times more reviews than the mid rated restaurants.
	
	*/

	SELECT SUM(review_count),
		CASE
			WHEN b.stars >=4
				THEN "High rating"
			WHEN b.stars >=2 & b.stars <= 3
				THEN "Mid rating"
			ELSE "Low rating"
		END AS rating
	FROM business b
		JOIN category c ON b.id = c.business_id
	WHERE rating != "Low rating" AND b.city="Toronto" AND c.category="Restaurants"
	GROUP BY rating;
    
/* 	+-------------------+-------------+
	| SUM(review_count) | rating      |
	+-------------------+-------------+
	|               206 | High rating |
	|                93 | Mid rating  |
	+-------------------+-------------+
	 */
	
/* iii. Are you able to infer anything from the location data provided between these two groups? Explain.

SQL code used for analysis:
 */
	/* According to the wiki page for canadian postal codes (https://en.wikipedia.org/wiki/Postal_codes_in_Canada) the first three characters refer to the forward sortation area, which breaks down into M#? (where # is a number and ? is a character) -- M in this case refers to Toronto, whereas # is whether it is rural (0) or urban (1-9), and finally ? refers to a specific region, city, or section depending on the areas size. If we pull just the first two characters, and group by rating, it looks like there are 3 high rated and 3 mid rated restaurants in the M3 area, 2 high rated in M2, and 1 each in M4 and M9. This probably means M6 is downtown since this area would have a higher concentration of restaurants.
	*/
	
	SELECT COUNT(*),
    substr(postal_code,1,2) AS post,
		CASE
			WHEN b.stars >=4
				THEN "High rating"
			WHEN b.stars >=2 & b.stars <= 3
				THEN "Mid rating"
			ELSE "Low rating"
		END AS rating
	FROM business b
		JOIN category c ON b.id = c.business_id
	WHERE rating != "Low rating" AND b.city="Toronto" AND c.category="Restaurants"
    GROUP BY post;
	
/* 	+----------+------+-------------+
	| COUNT(*) | post | rating      |
	+----------+------+-------------+
	|        2 | M2   | High rating |
	|        1 | M4   | Mid rating  |
	|        3 | M5   | Mid rating  |
	|        3 | M6   | High rating |
	|        1 | M9   | High rating |
	+----------+------+-------------+ */
	
		
/* 2. Group business based on the ones that are open and the ones that are closed. What differences can you find between the ones that are still open and the ones that are closed? List at least two differences and the SQL code you used to arrive at your answer.
		
i. Difference 1:
    Businesses that are still open seem to have more reviews and a higher rating, but if you look at the end of the table, closed wine & spirits and vape shops have both more reviews and higher or equal ratings. Beter tools would reveal more differences.  
         
ii. Difference 2:
    If you ignore the category and compare review count and average rating overall, there are many more reviews for open restaurants, but the rating is only 0.2 stars higher for open restaurants   
          */
         
SQL code used for analysis:

	SELECT is_open,
	AVG(b.review_count),
	AVG(b.stars),
	c.category
	FROM business b
	JOIN category c On b.id = c.business_id
	GROUP BY c.category, is_open
	ORDER BY category;

	SELECT COUNT(*),
	SUM(review_count),
	AVG(stars),
	is_open
	FROM business b
	JOIN category c On b.id = c.business_id
	GROUP BY is_open
	ORDER BY COUNT(*) DESC;


/* 3. For this last part of your analysis, you are going to choose the type of analysis you want to conduct on the Yelp dataset and are going to prepare the data for analysis.

Ideas for analysis include: Parsing out keywords and business attributes for sentiment analysis, clustering businesses to find commonalities or anomalies between them, predicting the overall star rating for a business, predicting the number of fans a user will have, and so on. These are just a few examples to get you started, so feel free to be creative and come up with your own problem you want to solve. Provide answers, in-line, to all of the following:
	
i. Indicate the type of analysis you chose to do:
    I want to see if there's an association between review type (useful, funny, cool) and number of stars     
         
ii. Write 1-2 brief paragraphs on the type of data you will need for your analysis and why you chose that data:
    
	I'll join the business and review tables, refactor the group types to be one column, and average the stars by review type	
                  
iii. Output of your finished dataset:
    +--------------------+-----------+
	|         AVG(stars) | f         |
	+--------------------+-----------+
	| 3.3582089552238807 | funny     |
	| 3.7854962764009277 | not funny |
	+--------------------+-----------+     
     

	+--------------------+------------+
	|         AVG(stars) | f          |
	+--------------------+------------+
	| 3.8244689889549703 | not useful |
	| 3.5419198055893073 | useful     |
	+--------------------+------------+
	
	+--------------------+----------+
	|         AVG(stars) | f        |
	+--------------------+----------+
	| 3.9070167886353855 | cool     |
	|  3.648039598801615 | not cool |
	+--------------------+----------+
	
	Looks like funny and useful reviews tend to have lower average ratings, presumably because the review is either roasting the establishment to get a funny rating, or warning potential customers of drawbacks of using their services, leading to a lower average rating. Cool reviews have higher average ratings, presumably because they're sharing interesting or unique info about the service which led to a positive experience
	 */
	 
iv. Provide the SQL code you used to create your final dataset:

	SELECT AVG(stars),
	CASE
		WHEN funny > 0 THEN 'funny'
		ELSE 'not funny'
	END AS f
	FROM review
	GROUP BY f;

	SELECT AVG(stars),
	CASE
		WHEN useful > 0 THEN 'useful'
		ELSE 'not useful'
	END AS f
	FROM review
	GROUP BY f;

	SELECT AVG(stars),
	CASE
		WHEN cool > 0 THEN 'cool'
		ELSE 'not cool'
	END AS f
	FROM review
	GROUP BY f;
