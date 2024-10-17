-- Tạo external table từ GCS
CREATE OR REPLACE EXTERNAL TABLE green_taxi_2022.external_greentaxi_2022
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://green_taxi_2022/*']
);

-- Tạo materialized table
CREATE OR REPLACE TABLE green_taxi_2022.greentaxi_2022
AS SELECT * FROM green_taxi_2022.external_greentaxi_2022;

-- Khoảng cách di chuyển trung bình 
SELECT AVG(trip_distance) AS avg_trip_distance
FROM green_taxi_2022.external_greentaxi_2022;

SELECT AVG(trip_distance) AS avg_trip_distance
FROM green_taxi_2022.greentaxi_2022;
# Truy vấn bảng external_greentaxi_2022 tài nguyên ước tính bằng 0B.
# Truy vấn bảng external_greentaxi_2022 tài nguyên ước tính bằng 5.7 MB.

-- Tổng số hành khách
SELECT SUM(passenger_count) AS total_passenger_count
FROM `green_taxi_2022.external_greentaxi_2022`;

SELECT SUM(passenger_count) AS total_passenger_count
FROM `green_taxi_2022.greentaxi_2022`;

-- 10 điểm đón có doanh thu cao nhất
SELECT pulocationid, SUM(total_amount) AS total_revenue
FROM `green_taxi_2022.external_greentaxi_2022`
GROUP BY pulocationid
ORDER BY total_revenue DESC
LIMIT 10;

SELECT pulocationid, SUM(total_amount) AS total_revenue
FROM `green_taxi_2022.greentaxi_2022`
GROUP BY pulocationid
ORDER BY total_revenue DESC
LIMIT 10;
# external table không tốn tài nguyên truy vấn, tốc độ truy vấn 938 ms
# marterialized table truy vấn tài nguyên tiêu tốn 11.39 MB, tốc độ truy vấn 616 ms

-- Thời gian di chuyển trung bình
SELECT AVG(TIMESTAMP_DIFF(lpep_dropoff_datetime, lpep_pickup_datetime, MINUTE)) AS avg_trip_duration_minutes
FROM green_taxi_2022.greentaxi_2022;

SELECT AVG(TIMESTAMP_DIFF(lpep_dropoff_datetime, lpep_pickup_datetime, MINUTE)) AS avg_trip_duration_minutes
FROM green_taxi_2022.external_greentaxi_2022;

-- Patition và Cluster
-- Partition theo lpep_pickup_datetime và Cluster theo PUlocationID
CREATE OR REPLACE TABLE green_taxi_2022.green_taxi_partitioned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM green_taxi_2022.greentaxi_2022;

-- Cluster theo lpep_pickup_datetime và Partition theo PUlocationID
CREATE OR REPLACE TABLE green_taxi_2022.greentaxi_clustered_partitioned
PARTITION BY PUlocationID
CLUSTER BY lpep_pickup_datetime AS
SELECT * FROM green_taxi_2022.greentaxi_2022;
# Không thực hiện được do PARTITION BY chỉ hỗ trợ DATE, DATETIME, hoặc TIMESTAMP.

-- Partition theo lpep_pickup_datetime và không sử dụng Cluster
CREATE OR REPLACE TABLE green_taxi_2022.greentaxi_partitioned_only
PARTITION BY DATE(lpep_pickup_datetime) AS
SELECT * FROM green_taxi_2022.greentaxi_2022;

-- Không sử dụng Partition nhưng Cluster theo PUlocationID
CREATE OR REPLACE TABLE green_taxi_2022.greentaxi_clustered_only
CLUSTER BY PUlocationID AS
SELECT * FROM green_taxi_2022.greentaxi_2022;

-- So sánh sự hiệu quả giữa các phương pháp phân vùng và phân cụm
-- Bảng greentaxi_partitioned_clustered
SELECT PUlocationID, COUNT(*) AS trip_count, SUM(total_amount) AS total_revenue
FROM `green_taxi_2022.green_taxi_partitioned_clustered`
WHERE lpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-12-31'
GROUP BY PUlocationID
ORDER BY trip_count DESC;

-- Bảng greentaxi_partitioned_only
SELECT PUlocationID, COUNT(*) AS trip_count, SUM(total_amount) AS total_revenue
FROM `green_taxi_2022.greentaxi_partitioned_only`
WHERE lpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-12-31'
GROUP BY PUlocationID
ORDER BY trip_count DESC;

-- Bảng greentaxi_clustered_only
SELECT PUlocationID, COUNT(*) AS trip_count, SUM(total_amount) AS total_revenue
FROM `green_taxi_2022.greentaxi_clustered_only`
WHERE lpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-12-31'
GROUP BY PUlocationID
ORDER BY trip_count DESC;