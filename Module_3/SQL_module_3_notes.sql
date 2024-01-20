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

/* #if we give it a braodcast hint (note the syntax), the run time is 6.01 sec
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