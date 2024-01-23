#################################
/* Module 3

Performed on Databricks servers
*/


/* 
Mount the dataset
Create database and call database 
Create table, make sure the columns can be assigned the correct data type; default is string
*/
%run ../Includes/Classroom-Setup
CREATE DATABASE IF NOT EXISTS Databricks

USE Databricks

DROP TABLE IF EXISTS fireCalls;

CREATE TABLE IF NOT EXISTS fireCalls
USING csv
OPTIONS (
  header "true",
  path "/mnt/davis/fire-calls/fire-calls-truncated-comma.csv",
  inferSchema "true"
)




/* Module 3 Week 2 */

/* #how long does it take to count all records?
#2.86 sec */
SELECT count(*) FROM fireCalls

/* #caching the table */
CACHE TABLE fireCalls

/* #Count again
#0.72 sec */
SELECT count(*) FROM fireCalls

/* #Lazy cache; only stores what is needed, i.e., the first 100 rows */
CACHE LAZY TABLE fireCalls
SELECT * FROM fireCalls LIMIT 100

/* #if we call a command that forces all data to be looked at, the lazy cache will cache all rows
 #takes a while the first run, but is very quick the second run (becasue it's been cached)
 */
SELECT count(*) FROM fireCalls 

/* Expand out the Spark job below. It should have:

1 stage with 8 tasks
1 stage with 200 tasks
The number assigned to the Job/Stage will depend on how many Spark jobs you have already executed on your cluster.
 */
SELECT `call type`, count(*) AS count
FROM firecalls
GROUP BY `call type`
ORDER BY count DESC


/* #setting partition to <200 (the default) can make the query run faster since only so much can be parallelized */
SET spark.sql.shuffle.partitions=8



/* Enable Adaptive Query Execution (AQE) to reduce the runtime of your queries
--Works by trying to partition data into similar sizes.
--ex. If you have a small dataset (~8mb) that you're joining to a large dataset, you can send the smaller dataset to all partitions to perform the join (braodcasting) rather than using both datasets to join

Skew Joins
--Data skew (some partitions can have much large data needs than others) can cause performance loss, but Spark can choose to break up large skewed data into smaller subpartitions

#run time is 5.94 sec */
SET spark.sql.adaptive.enabled = FALSE

/* #run time is 1.18 sec
#chose only one partition for efficiency purposes */
SET spark.sql.adaptive.enabled = TRUE

/* #joining different sized data
#Note that fireCalls is a much smaller dataset with 240,316 records vs fireCallsParquet which contains 4,799,622 records.
#took 55.08 sec */
SELECT * 
FROM fireCalls 
JOIN fireCallsParquet on fireCalls.`Call Number` = fireCallsParquet.`Call_Number`

/* #if we give it a broadcast hint (note the syntax), the run time is 6.01 sec
#we can check the size of the tables queried in the SQL tab of the Spark Jobs view */
SELECT /*+ BROADCAST(fireCalls) */ * 
FROM fireCalls 
JOIN fireCallsParquet on fireCalls.`Call Number` = fireCallsParquet.`Call_Number`



/*
#Module 3

*/
/* 
#connect to AWS via python and mount to the data
 */
%python
ACCESS_KEY = "AKIAJBRYNXGHORDHZB4A"
/* # Encode the Secret Key to remove any "/" characters */
SECRET_KEY = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF".replace("/", "%2F")
AWS_BUCKET_NAME = "davis-dsv1071/data/"
MOUNT_NAME = "/mnt/davis-tmp"

%python
try:
  dbutils.fs.mount("s3a://{}:{}@{}".format(ACCESS_KEY, SECRET_KEY, AWS_BUCKET_NAME), MOUNT_NAME)
except:
  print("""{} already mounted. Unmount using `dbutils.fs.unmount("{}")` to unmount first""".format(MOUNT_NAME, MOUNT_NAME))
  
/* #check mount */
%fs ls /mnt/davis-tmp
%fs mounts

/* #unmount */
%python
dbutils.fs.unmount("/mnt/davis-tmp")


/*
Java Database Connectivity (JDBC) is an application programming interface (API) that defines database connections in Java environments. Spark is written in Scala, which runs on the Java Virtual Machine (JVM). This makes JDBC the preferred method for connecting to data whenever possible. Hadoop, Hive, and MySQL all run on Java and easily interface with Spark clusters.
*/

/* #get correct drivers */
%scala
Class.forName("org.postgresql.Driver")

/* #create sql table of twitter data */
DROP TABLE IF EXISTS twitterJDBC;

CREATE TABLE IF NOT EXISTS twitterJDBC
USING org.apache.spark.sql.jdbc
OPTIONS (
  driver "org.postgresql.Driver",
  url "jdbc:postgresql://server1.databricks.training:5432/training",
  user "readonly",
  password "readonly",
  dbtable "training.Account"
)

/* #can add subqueries to db to only return specific results */
DROP TABLE IF EXISTS twitterPhilippinesJDBC;

CREATE TABLE twitterPhilippinesJDBC
USING org.apache.spark.sql.jdbc
OPTIONS (
  driver "org.postgresql.Driver",
  url "jdbc:postgresql://server1.databricks.training:5432/training",
  user "readonly",
  password "readonly",
  dbtable "(SELECT * FROM training.Account WHERE location = 'Philippines') as subq"
)

/*
Parallelizing the query
*/

/* #Selecting min and max user id to use as input for the extraction */
SELECT min(userID) as minID, max(userID) as maxID from twitterJDBC
/* 
#Parallel read */
DROP TABLE IF EXISTS twitterParallelJDBC;

CREATE TABLE IF NOT EXISTS twitterParallelJDBC
USING org.apache.spark.sql.jdbc
OPTIONS (
  driver "org.postgresql.Driver",
  url "jdbc:postgresql://server1.databricks.training:5432/training",
  user "readonly",
  password "readonly",
  dbtable "training.Account",
  partitionColumn '"userID"',
  lowerBound 2591,
  upperBound 951253910555168768,
  numPartitions 25
)

/* #we can time the task with this syntax */
%python
%timeit sql("SELECT * from twitterJDBC").describe()


/* ###
#3.4 - File Formats
 */
/* #we look at a json file, and convert to csv by separating by ":" */
%fs head --maxBytes=1000 /mnt/davis/fire-calls/fire-calls-colon.txt
CREATE OR REPLACE TEMPORARY VIEW fireCallsCSV
USING CSV 
OPTIONS (
    path "/mnt/davis/fire-calls/fire-calls-colon.txt",
    header "true",
    sep ":"
  )
  
/* #If we infer the schema, the run time increases 50x, BUT the data types are listed (though it's probably better to just hard code these in) */
CREATE OR REPLACE TEMPORARY VIEW fireCallsCSV
USING CSV 
OPTIONS (
    path "/mnt/davis/fire-calls/fire-calls-colon.txt",
    header "true",
    sep ":",
    inferSchema "true"
  )
  
/* #if we want to read in compressed files, we can! They give examples of gzip and bzip formats
#Inferring the schema from compressed formats takes much longer than uncompressed, but the tradeoff is in file size. 
#Bzip is splittable (not 100% sure what this means or why it makes bzip partitionable), so we can assign multiple partitions to get the read step to go quicker
#parquet format is in a column format (think casted rather than melted) and it stored both the data and metadata, so reading and getting the schema is fast

#delta files are another column format that is also splittable. We can request more partitions like so: */
DROP TABLE IF EXISTS fireCallsDelta;

CREATE TABLE fireCallsDelta
USING delta
AS
  SELECT /*+ REPARTITION(8) */ *
  FROM fireCallsParquet
 
/* #REPARTITON is like COALESCE in Spark, the difference being COALECE is for narrow data, while REPARTITION is for wide data
#If we compare the time it takes to read a csv vs delta file, the csv takes much longer (37s) compared to the delta file (6.83)
 */
SELECT count(`Incident Number`)
FROM firecallsCSV
WHERE Priority > 1

SELECT count(`Incident_Number`)
FROM fireCallsDelta
WHERE Priority > 1

/* #some notes copy/pasted from the course:
 */

/* Type	Inference Type	Inference Speed	Reason					Should Supply Schema?
CSV		Full-Data-Read	Slow			File size				Yes
Parquet	Metadata-Read	Fast/Medium		Number of Partitions	No (most cases)
Tables	n/a				n/a				Predefined				n/a
JSON	Full-Read-Data	Slow			File size				Yes
Text	Dictated		Zero			Only 1 Column			Never
JDBC	DB-Read			Fast			DB Schema				No


Reading CSV
spark.read.csv(..)
-There are a large number of options when reading CSV files including headers, column separator, escaping, etc.
-We can allow Spark to infer the schema at the cost of first reading in the entire file
-Large CSV files should always have a schema pre-defined

Reading Parquet
spark.read.parquet(..)
-Parquet files are the preferred file format for big-data
-It is a columnar file format
-It is a splittable file format
-It offers a lot of performance benefits over other formats including predicate push down
-Unlike CSV, the schema is read in, not inferred
-Reading the schema from Parquet's metadata can be extremely efficient

Reading Tables
spark.read.table(..)
-The Databricks platform allows us to register a huge variety of data sources as tables via the Databricks UI
-Any DataFrame (from CSV, Parquet, whatever) can be registered as a temporary view
-Tables/Views can be loaded via the DataFrameReader to produce a DataFrame
-Tables/Views can be used directly in SQL statements

Reading JSON
spark.read.json(..)
-JSON represents complex data types unlike CSV's flat format
-Has many of the same limitations as CSV (needing to read the entire file to infer the schema)
-Like CSV has a lot of options allowing control on date formats, escaping, single vs. multiline JSON, etc.

Reading Text
spark.read.text(..)
-Reads one line of text as a single column named value
-Is the basis for more complex file formats such as fixed-width text files

Reading JDBC
spark.read.jdbc(..)
-Requires one database connection per partition
-Has the potential to overwhelm the database
-Requires specification of a stride to properly balance partitions
 */



/* 
#3.5 Schemas and Types
Overall:
Schemas with Semi-Structured JSON Data
Tabular data, such as that found in CSV files or relational databases, has a formal structure where each observation, or row, of the data has a value (even if it's a NULL value) for each feature, or column, in the data set.

Semi-structured data does not need to conform to a formal data model. Instead, a given feature may appear zero, once, or many times for a given observation.

Semi-structured data storage works well with hierarchical data and with schemas that may evolve over time. One of the most common forms of semi-structured data is JSON data, which consists of attribute-value pairs.


#json formats are in key:value format, and we can read them in like a csv */
CREATE OR REPLACE TEMPORARY VIEW fireCallsJSON
USING JSON 
OPTIONS (
    path "/mnt/davis/fire-calls/fire-calls-truncated.json"
  )
  
/* #inferring the schema:
While there are similarities between reading in CSV & JSON there are some key differences:

We only need one job even when inferring the schema.
There is no header which is why there isn't a second job in this case - the column names are extracted from the JSON object's attributes.
Unlike CSV which reads in 100% of the data, the JSON reader only samples the data.

#user-defined schemas */
CREATE OR REPLACE TEMPORARY VIEW fireCallsJSON ( 
  `Call Number` INT,
  `Unit ID` STRING,
  `Incident Number` INT,
  `Call Type` STRING,
  `Call Date` STRING,
  `Watch Date` STRING,
  `Received DtTm` STRING,
  `Entry DtTm` STRING,
  `Dispatch DtTm` STRING,
  `Response DtTm` STRING,
  `On Scene DtTm` STRING,
  `Transport DtTm` STRING,
  `Hospital DtTm` STRING,
  `Call Final Disposition` STRING,
  `Available DtTm` STRING,
  `Address` STRING,
  `City` STRING,
  `Zipcode of Incident` INT,
  `Battalion` STRING,
  `Station Area` STRING,
  `Box` STRING,
  `Original Priority` STRING,
  `Priority` STRING,
  `Final Priority` INT,
  `ALS Unit` BOOLEAN,
  `Call Type Group` STRING,
  `Number of Alarms` INT,
  `Unit Type` STRING,
  `Unit sequence in call dispatch` INT,
  `Fire Prevention District` STRING,
  `Supervisor District` STRING,
  `Neighborhooods - Analysis Boundaries` STRING,
  `Location` STRING,
  `RowID` STRING
)
USING JSON 
OPTIONS (
    path "/mnt/davis/fire-calls/fire-calls-truncated.json"
)


/* #3.6 - Writing Data
#so, in Spark, they have us switch to python to write the files in sql
 */
%python
df = sql("SELECT * FROM fireCallsCSV")
display(df)

#OR

%python
sql("SELECT * FROM fireCallsCSV8p").write.mode("OVERWRITE").csv(username + "/fire-calls-repartitioned.csv")

/* #note the use of OVERWRITE in .mode to overwrite a file if it already exists

 */
 
/* #3.7 - Tables and Views
There are a lot of concepts when it comes to different options for handling data in various SQL environments. One primary disctinction is between a table and view:

A table creates a table in an existing database
A view is a SQL query stored in a database
When we create a table, we're writing data to a database. In our case, we're writing to the Databricks File System and storing the metadata in the Hive Metastore, a common metastore in various environments. A view, by contrast, is just saving the query itself. We have to recalculate this query each time we call the view, which can take more time but it will also give us the most current data.

Also, views and tables are scoped in different ways:
A global view or table is available across all clusters
A temporary view or table is available only in the notebook you're working in

#Essentially, views are quick to make, but you have to make them every time you run a query from the view. Tables take longer to make (since they're being written to drive) but are quick to query because the database doesnt have to be built every time; the computation is done


#managed vs unmanaged tables
A managed table is a table that manages both the data itself as well as the metadata. In this case, a DROP TABLE command removes both the metadata for the table as well as the data itself.

Unmanaged tables manage the metadata from a table such as the schema and data location, but the data itself sits in a different location, often backed by a blob store like the Azure Blob or S3. Dropping an unmanaged table drops only the metadata associated with the table while the data itself remains in place. */


/* Managed table */
CREATE TABLE tableManaged (
  var1 INT,
  var2 INT
);


/* Unmanaged table */
CREATE EXTERNAL TABLE tableUnmanaged (
  var1 INT,
  var2 INT
)
STORED AS parquet
LOCATION '/tmp/unmanagedTable'


INSERT INTO tableManaged
  VALUES (1, 1), (2, 2)
  




/* Module 4 - Data Lakes, Warehouses, and Lakehouses */

/* Data warehouses provide a very limited support for more advanced machine learning workloads. Given these challenges, much of the industry turned to data lakes on scalable Cloud storage like AWS's S3, and Azure's Blob Storage. They are cheap, they're scalable, and they support a wide variety of data formats. 

However, they're also very difficult to manage, and they can often turn into data swamps due to difficulty in extracting value from them. Many enterprises would have a two-storage solution. Raw data would land in the data lake as it is cheap and infinitely scalable, then the business-critical data would be copied into a data warehouse for queries into power dashboards. However, this would lead to stale, inconsistent data as the data warehouses could only be updated with new data upon running some scheduled job. This is where lakehouses come in. 

Lakehouses combine the scalability and low-cost storage of data lakes with the speed and reliability and ACID transactional guarantees of data warehouses. A lakehouse architecture augments your data lake with metadata management for optimized performance. No need to unnecessarily copy data to or from a data warehouse. You get data versioning, reliable and fault-tolerant transactions, and a fast query engine all while maintaining open standards. It ensures that your data teams can access timely, reliable, high-quality data.  

Delta Lake is an open-source project from the Linux Foundation that supports the lakehouse architecture and has the tightest integration with Apache Spark among all of the open-source projects.

Delta Lake is built on top of Parquet, a file format you've already been introduced to, but it adds an additional transaction log. The transaction log tracks the changes or the delta in your underlying data.
*/


/* 
While databases are used for online transactions, data warehouses are common for all sorts of business intelligence or BI applications, like nightly reports for various business outcomes. There are clear downsides to data warehouses though. 

First, data warehouses are often expensive and closed source. Second, they really only support really structured data. You couldn't work with images, videos, or other unstructured data formats. Semi-structured data types like JSON also pose some challenges. 

Finally, these systems were intended for reporting, not more advanced workflows like machine learning. Enter data lakes. Recall that data warehouses were really created for your own custom data center. Then the Cloud came about where anybody could rent resources in a flexible way from companies like Amazon and Microsoft. 

This means, among other things, scalable and cheap storage. This enabled data lakes, where you can land data in any format you need. They support machine learning workloads because they're so highly flexible. You could better integrate with open formats like Parquet and Delta. 

But there are downsides too. Namely, how can you ensure that your data is as you would want it to be? If you can't trust that data, you'll have poor support for BI and poor ability to govern the data you do have by making sure it matches your expectations. Data lakes could easily become data swamps because of that flexibility. 

This is used in traditional RDBMS's or relational database management systems. The core idea here is that you are normalizing your data into different tables that you can join back together using keys. This reduces data redundancy and improves the integrity of your data. While this works well in many contexts, creating our data model can take a good deal of time, and updating that model often as challenges. 

Next up is NoSQL. These are non-relational models. There are many approaches out there, but document stores, like storing newspaper articles, for instance, or key value stores, are common NoSQL databases. 

Beyond relational and non-relational models, there's also the idea of doing query-first design. This is common in distributed environments. In this case, you model your data based on optimizing queries. If you know you'll need a user's name in multiple tables, it's okay to copy it to both locations because this could improve your query speed. 

Then finally, there are star schemas. These are used in data warehouses where you organize your data into fact and dimension tables. A fact could be an individual event like maybe a sales transaction, while dimension tables would have related information like geographical aspects, or time aspects.
 */


/* 
A Lakehouse combines the best of both worlds of Data Warehouses and Data Lakes, no need to manage multiple systems or experience stale, inconsistent, redundant data. Using a Lakehouse, you can have a single source of truth for your data, allowing you to move fast without breaking thing.

Lakehouses work with any sort of data, structured, semi-structured, and unstructured. We can then land them in our Data Lake with appropriate metadata management, caching, and indexing. 

Let's talk about some common data engineering problems: if you're working with a Data Lake, you're likely working on files or maybe backups of databases. It can be hard to append data to those files. Other modifications of that data like deleting data in a file is also difficult, and if the job fails partway through an operation, then you might have some corrupt files sitting around. Doing real-time operations like streaming can also be difficult, and keeping historical data versions is also costly if you have to keep a database dump for say, each day of the year.

Data kept in Lakes can also lack good performance with large metadata files, and if you have too many small files, queries against those files take longer as you need one thread to read each of those files, this is the so-called small files problem.

The Lakehouse is unique in a number of ways. First, it's a simple way to manage data as it only needs to exist once to support all of your workloads, it's not siloed based upon type of workload you're performing. It's also open, that means that it's based on open source software and open standards to make it easy to work with without having to engage with expensive proprietary formats. Finally, it's collaborative, meaning engineers, analysts, and data scientists work together easily to serve a number of different workloads. 

The backbone of the Lakehouse is Delta Lake, an open source software originally developed at Databricks and later open-sourced and donated to the Linux Foundation. That means that anybody can download the source code and use this framework to more efficiently manage their data applications. At a high level, Data Lakes have enhanced reliability by allowing for database or acid transactions against your data, more on this in the next video. It also has increased performance with indexing and partitioning in a bunch of related optimizations. There's improved governance with Table ACLs, an ACL is a so-called Access Control List, this is a common way of handling permissioning. Finally, you have better trust in your data through schema enforcement and other expectations.

 */

/* 
Delta adds a transaction log on top of Parquet files. This means that we can perform updates and delete rows of files. This specific term for this is ACID transactions. 

ACID is an acronym:
The A stands for atomicity or the guarantee that if you're adding data to one table and subtracting it from another, you can have that be a single transaction where both steps either succeed or fail together. 

The C stands for consistency, or that the database is always in a valid state. That means that if I start writing to the table while somebody else is reading from it, there will be a consistent view. 

Isolation is our I, and that means that we can do concurrent queries against our data. 

For our D, that's durability. This means that if the lights go out, we won't lose our data. In our case, this also means that if we take down our Spark cluster, then the data will persist. Those are our ACID guarantees. 

With Delta, you can evolve and enforce your schema as needed. You can also write from batch and streaming operations to Delta tables. Finally, since you have a log of all of the transactions at this table, you can always time travel back to previous versions of your data.

Now let's talk about the architecture. On the left-hand here, we have a number of different possible data sources, including data cues like Kafka and Kinesis, Firehose in a Data Lake and Spark. You can land the data however you need to. 

The data is then put into a bronze table, which is also called a raw or ingestion table. Basically, we just need a way to land our data and some raw format. We want it to be raw so that we can go back and see the source of our data in case something goes wrong. Now this bronze table would be a Delta table backed by S3 or Azure Blob. On this table, you'd want to do some schema enforcement so that the data has the schema you'd expect. If it fails this check, then you can quarantine your data so that it's not propagating bad data through your system. You can then go to the quarantine to figure out what went wrong. To keep organized, it's helpful to have bronze or raw in the prefix of the table or database name. 

Next up is the silver tables. This is also known as filtered or augmented data. This would be its own Delta table or tables with higher-quality data. You might parse out timestamps or pull out specific values you're expecting to see. 

Then finally, you'd have another set of tables known as gold tables. These are your high level aggregates. You might have a given table for a given report or dashboard, or as features for some machine learning model.
 */


/* 
4.4 Delta Lake
 */

/* Create Parquet file */
CREATE OR REPLACE TEMPORARY VIEW fireCallsParquet
USING parquet
OPTIONS (
  path "/mnt/davis/fire-calls/fire-calls-clean.parquet/"
)

/* 
Write to delta bronze */
CREATE DATABASE IF NOT EXISTS Databricks;
USE Databricks;
DROP TABLE IF EXISTS fireCallsBronze;

CREATE TABLE fireCallsBronze
USING DELTA
AS 
  SELECT * FROM fireCallsParquet

/* 
create and write to delta silver */
DROP TABLE IF EXISTS fireCallsSilver;

CREATE TABLE fireCallsSilver 
USING DELTA
AS 
  SELECT Call_Number, Call_Type, Call_Date, Received_DtTm, Address, City, Zipcode_of_Incident, Unit_Type, `Neighborhooods_-_Analysis_Boundaries`
  FROM fireCallsBronze
  WHERE (City IS NOT null) AND (`Neighborhooods_-_Analysis_Boundaries` <> "None");
  
SELECT * FROM fireCallsSilver LIMIT 10;

/* 
We can clean the data more by updating the silver table
 */

UPDATE fireCallsSilver SET City = "San Francisco" WHERE (City = "SF") OR (City = "SAN FRANCISCO")

/* 
we can look at the transaction log (version control) 
*/
DESCRIBE HISTORY fireCallsSilver


/* 
Make the delta gold table; note that each step cleans or adjusts the data for a cleaner output
 */
DROP TABLE IF EXISTS fireCallsGold;

CREATE TABLE fireCallsGold 
USING DELTA
AS 
  SELECT `Neighborhooods_-_Analysis_Boundaries` as Neighborhoods, Call_Type, count(*) as Count
  FROM fireCallsSilver
  GROUP BY Neighborhoods, Call_Type
  
  

/* 
4.5 Advanced Delta Lake
 */
 
 /* 
We can partition out by column using the PARTITIONED BY (City)

Very useful if you want to grab specific partions of data (e.g., from speicifc cities

Two general rules of thumb to follow, for deciding which column to partition by. 

The first one is the cardinality, of the column or the number of unique records for that column. So for example, if you're trying to partition based off of incident id, something that is unique for each record, you're going to have lots and lots of small files. So it's not actually going to give you any speed up because you have the overhead of all those small files. 

The other thing to consider is the amount of data you need to partition. You should partition by a column If you expect data on that partition to be at least one gigabyte. 
 */
 
CREATE DATABASE IF NOT EXISTS Databricks;
USE Databricks;
DROP TABLE IF EXISTS fireCallsDelta;

CREATE TABLE fireCallsDelta
USING DELTA
PARTITIONED BY (City)
AS 
  SELECT * FROM fireCallsParquet
 

/* 
Schema Enforcement & Evolution
Schema enforcement, also known as schema validation, is a safeguard to ensure data quality. Delta Lake uses schema validation on write, which means that all new writes to a table are checked for compatibility with the target table’s schema at write time. 

If the schema is not compatible, Delta Lake cancels the transaction altogether (no data is written), and raises an exception to let the user know about the mismatch.

Schema evolution is a feature that allows users to easily change a table’s current schema to accommodate data that is changing over time. Most commonly, it’s used when performing an append or overwrite operation, to automatically adapt the schema to include one or more new columns.

To determine whether a write to a table is compatible, Delta Lake uses the following rules. The DataFrame to be written:

Cannot contain any additional columns that are not present in the target table’s schema.
Cannot have column data types that differ from the column data types in the target table.
Cannot contain column names that differ only by case.
 */
 
 /* 
If we try to write a new column to a table, a schema error may pop up
 */
 
 INSERT OVERWRITE TABLE fireCallsDelta SELECT *, `Neighborhooods_-_Analysis_Boundaries` AS Neighborhoods FROM fireCallsDelta
 
 /* 
But we can use an auto merge function to try to get it to merge the data if the schemas are compatible and try the operations again
 */
 
SET spark.databricks.delta.schema.autoMerge.enabled=TRUE
INSERT OVERWRITE TABLE fireCallsDelta SELECT *, `Neighborhooods_-_Analysis_Boundaries` AS Neighborhoods FROM fireCallsDelta;

SELECT * FROM fireCallsDelta
 
 
 /* 
We can query from past versions of a data SET
 */
SELECT * 
FROM fireCallsDelta 
  VERSION AS OF 0

SELECT * 
FROM fireCallsDelta 
  VERSION AS OF 1  
 
 
 /* 
Deleting records (for individuals for legals reasons)
 */
DELETE FROM fireCallsDelta WHERE Incident_Number = "14055109";

 




/* 
ML models
 */
 
 USE DATABRICKS;

CREATE TABLE IF NOT EXISTS fireCallsClean
USING parquet
OPTIONS (
  path "/mnt/davis/fire-calls/fire-calls-clean.parquet"
)

/* 
Let's convert our Response_DtTm and Received_DtTm into timestamp types.
 */
CREATE OR REPLACE VIEW time AS (
  SELECT *, unix_timestamp(Response_DtTm, "MM/dd/yyyy hh:mm:ss a") as ResponseTime, 
            unix_timestamp(Received_DtTm, "MM/dd/yyyy hh:mm:ss a") as ReceivedTime
  FROM fireCallsClean
)

/* 
Now that we have our Response_DtTm and Received_DtTm as timestamp types, we can compute the difference in minutes between the two.
 */
CREATE OR REPLACE VIEW timeDelay AS (
  SELECT *, (ResponseTime - ReceivedTime)/60 as timeDelay
  FROM time
)
 
 /* We have some records with a negative time delay, and some with very extreme values. We will filter out those records.
 */
SELECT timeDelay, Call_Type, Fire_Prevention_District, `Neighborhooods_-_Analysis_Boundaries`, Unit_Type
FROM timeDelay 
WHERE timeDelay < 0

SELECT timeDelay, Call_Type, Fire_Prevention_District, `Neighborhooods_-_Analysis_Boundaries`, Unit_Type
FROM timeDelay 
WHERE timeDelay < 15 AND timeDelay > 0
 
 /* 
Convert to Pandas DataFrame
We are going to convert our Spark DataFrame to a Pandas DataFrame to build a scikit-learn model. Although we could use SparkML to train models, a lot of data scientists start by building their models using Pandas and Scikit-Learn.

We will also enable Apache Arrow for faster transfer of data from Spark DataFrames to Pandas DataFrames.
 */
 
%python
pdDF = sql("""SELECT timeDelay, Call_Type, Fire_Prevention_District, `Neighborhooods_-_Analysis_Boundaries`, Unit_Type
              FROM timeDelay 
              WHERE timeDelay < 15 AND timeDelay > 0""").toPandas()
			  
 /* 
Let's visualize the distribution of our time delay.
 */
%python
import pandas as pd
import numpy as np

fig = pdDF.hist(column="timeDelay")[0][0]
display(fig.figure)

/* 
In this notebook we are going to use 80% of our data to train our model, and 20% to test our model. We set a random_state for reproducibility.
 */
%python
from sklearn.model_selection import train_test_split

X = pdDF.drop("timeDelay", axis=1)
y = pdDF["timeDelay"].values
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42) 
 

/* 
Before we get started building our linear regression model, let's establish our baseline RMSE on our test dataset by always predicting the average value. Here, we are going to take the square root of the MSE.
 */
%python
from sklearn.metrics import mean_squared_error
import numpy as np

avgDelay = np.full(y_test.shape, np.mean(y_train), dtype=float)

print("RMSE is {0}".format(np.sqrt(mean_squared_error(y_test, avgDelay))))

 /* 
Build Linear Regression Model
Great! Now that we have established a baseline, let's use scikit-learn's pipeline API to build a linear regression model.

Our pipeline will have two steps: 0. One Hot Encoder: this converts our categorical features into numeric features by creating a dummy column for each value in that category. * For example, if we had a column called Animal with the values Dog, Cat, and Bear, the corresponding one hot encoding representation for Dog would be: [1, 0, 0], Cat: [0, 1, 0], and Bear: [0, 0, 1] 0. Linear Regression model: find the line of best fit for our training data
 */
 
%python
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline

ohe = ("ohe", OneHotEncoder(handle_unknown="ignore"))
lr = ("lr", LinearRegression(fit_intercept=True, normalize=True))

pipeline = Pipeline(steps = [ohe, lr]).fit(X_train, y_train)
y_pred = pipeline.predict(X_test)

/* 

 */
%python
print(pipeline.steps[0][1].get_feature_names())


/* 

 */
%python
coefs = pd.DataFrame([pipeline.steps[0][1].get_feature_names(), pipeline.steps[1][1].coef_]).T
coefs.columns = ["features", "coef"]
coefs = coefs.sort_values("coef", ascending=False)

coefs.head(10)


/* 

 */
%python
coefs.sort_values("coef", ascending=True).head(10)


/* 

 */
%python
from sklearn.metrics import mean_squared_error

print("RMSE is {0}".format(np.sqrt(mean_squared_error(y_test, y_pred))))


 /* 

Save Model
Not bad! We did a bit better than our baseline model.

Let's save this model using MLflow. MLflow is an open-source project created by Databricks to help simplify the Machine Learning life cycle.

While MLflow is out of the scope of this class, it has a nice function to generate Spark User-Defined Function (UDF) to apply this model in parallel to the rows in our dataset. We will see this in the next notebook.
 */
 
%python
try:
  import mlflow
  import mlflow.sklearn

  with mlflow.start_run(run_name="Basic Linear Regression Experiment") as run:
    mlflow.sklearn.log_model(pipeline, "Model Pipeline")
except:
  print("ERROR: This cell did not run, likely because you're not running the correct version of software. Please use a cluster with a runtime with `ML` at the end of the version name.")
 