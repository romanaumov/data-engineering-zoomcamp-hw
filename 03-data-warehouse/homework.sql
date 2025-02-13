-- Create external table
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny.tripdata_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://roman_de_zoomcamp_hw3_2025_parquet/yellow_tripdata_2024-*.parquet']
);

-- Create a regular table
CREATE OR REPLACE TABLE `taxi-rides-ny.tripdata` AS
SELECT *
FROM `taxi-rides-ny.tripdata_external`;



-- Question 1: What is count of records for the 2024 Yellow Taxi Data?
SELECT count(*) FROM `taxi-rides-ny.tripdata`;
--Answer: 

-- Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT COUNT(DISTINCT(PULocationIDs)) FROM `taxi-rides-ny.tripdata_external`;
--Answer: 
SELECT COUNT(DISTINCT(PULocationIDs)) FROM `taxi-rides-ny.tripdata`;
--Answer: 

-- Question 3: Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. 
-- Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
SELECT PULocationID
FROM `taxi-rides-ny.tripdata`
LIMIT 10;
--Answer: 

SELECT PULocationID, DOLocationID
FROM `taxi-rides-ny.tripdata`
LIMIT 10;
--Answer: 

--Answer: "BigQuery is a columnar database, and it only scans the specific columns requested in the query. 
-- Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed."

--Question 4: How many records have a fare_amount of 0?

SELECT COUNT(*) AS zero_fare_count
FROM `taxi-rides-ny.tripdata`
WHERE fare_amount = 0;
--Answer: 


-- Question 5: What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

CREATE OR REPLACE TABLE `taxi-rides-ny.partitioned_tripdata`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `taxi-rides-ny.tripdata`;


--Answer: "Partition by tpep_dropoff_datetime and Cluster on VendorID"

-- Question 6: Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
-- Use the materialized table you created earlier in your from clause and note the estimated bytes. 
-- Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

SELECT DISTINCT VendorID
FROM `taxi-rides-ny.tripdata`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
-- Answer: 

SELECT DISTINCT VendorID
FROM `taxi-rides-ny.partitioned_tripdata`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
-- Answer:

-- Question 7: Where is the data stored in the External Table you created?
--Answer: GCP Bucket. BigQuery only stores the metadata, while the actual data remains in the GCP Bucket

-- Question 8: It is best practice in Big Query to always cluster your data?
-- Answer: False. For small datasets or queries that don’t benefit from the clustered columns, clustering may not improve performance.
