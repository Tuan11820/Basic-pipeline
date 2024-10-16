CREATE EXTERNAL TABLE homework2-436406.green_taxi.external_greendata_2022
OPTIONS (
  format = 'parquet',
  uris = ['gs://green_taxi_2022/*']
);

SELECT DISTINCT COUNT(fare_amount) FROM green_taxi.external_greendata_2022
WHERE fare_amount = 0;


CREATE OR REPLACE TABLE `homework2-436406.green_taxi.optimized_table`
PARTITION BY DATE(lpep_pickup_datetime)  -- Phân vùng theo ngày của lpep_pickup_datetime
CLUSTER BY PUlocationID  -- Cụm hóa theo PUlocationID
AS
SELECT *
FROM `green_taxi.external_greendata_2022`;

SELECT * FROM `green_taxi.external_greendata_2022` LIMIT 10;


