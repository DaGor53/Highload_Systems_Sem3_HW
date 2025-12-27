import time
from pyspark.sql import SparkSession

time.sleep(30)

print("BUGAGAGSpark session started successfully")

spark = SparkSession.builder \
    .appName("PostgreSQL Connection Test") \
    .getOrCreate()

url = "jdbc:postgresql://postgres:5432/my_db"

properties = {
    "user": "my_user",
    "password": "12345",
    "driver": "org.postgresql.Driver"
}

table_name = "mock_data" 

print("Trying to read table:", table_name)

df = spark.read.jdbc(
    url=url,
    table=table_name,
    properties=properties
)

df.show(5)

print("Row count:", df.count())

ch_url = "jdbc:clickhouse://clickhouse:8123/mydatabase"

ch_properties = {
    "user": "click",
    "password": "click",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

ch_test_query = "(SELECT 1 AS ok) AS t"

print("Checking ClickHouse connection...")

ch_df = spark.read.jdbc(
    url=ch_url,
    table=ch_test_query,
    properties=ch_properties
)

ch_df.show()

print("ClickHouse connection OK")

spark.stop()
print("Spark session stopped")