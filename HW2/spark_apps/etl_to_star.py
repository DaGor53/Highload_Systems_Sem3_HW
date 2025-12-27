from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, row_number, year, month, dayofmonth)
from pyspark.sql.functions import monotonically_increasing_id, year, month, dayofmonth
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
from pyspark import StorageLevel
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType
import time

time.sleep(40)

spark = (
    SparkSession.builder
    .appName("etl_to_star")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

PG_URL = "jdbc:postgresql://postgres:5432/my_db"
PG_PROPS = {
    "user": "my_user",
    "password": "12345",
    "driver": "org.postgresql.Driver",
    "batchsize": "1000"
}

mock_df = spark.read.jdbc(
    url=PG_URL,
    table="mock_data",
    column="id",        
    lowerBound=1,
    upperBound=1_000_000,
    numPartitions=10,
    properties=PG_PROPS
)

dim_customer = (
    mock_df.select(
        col("customer_first_name"),
        col("customer_last_name"),
        col("customer_age"),
        col("customer_email"),
        col("customer_country"),
        col("customer_postal_code"),
        col("customer_pet_type"),
        col("customer_pet_name"),
        col("customer_pet_breed")
    )
    .dropDuplicates()
)

dim_seller = (
    mock_df.select(
        col("seller_first_name"),
        col("seller_last_name"),
        col("seller_email"),
        col("seller_country"),
        col("seller_postal_code")
    )
    .dropDuplicates()
)

dim_product = (
    mock_df.select(
        col("product_name"),
        col("product_category"),
        col("product_price"),
        col("product_quantity"),
        col("pet_category"),
        col("product_weight"),
        col("product_color"),
        col("product_size"),
        col("product_brand"),
        col("product_material"),
        col("product_description"),
        col("product_rating"),
        col("product_reviews"),
        col("product_release_date"),
        col("product_expiry_date")
    )
    .dropDuplicates()
)

dim_store = (
    mock_df.select(
        col("store_name"),
        col("store_location"),
        col("store_city"),
        col("store_state"),
        col("store_country"),
        col("store_phone"),
        col("store_email")
    )
    .dropDuplicates()
)

dim_supplier = (
    mock_df.select(
        col("supplier_name"),
        col("supplier_contact"),
        col("supplier_email"),
        col("supplier_phone"),
        col("supplier_address"),
        col("supplier_city"),
        col("supplier_country")
    )
    .dropDuplicates()
)

dim_date = (mock_df.select(col("sale_date").alias("date")).dropDuplicates())

dim_customer = dim_customer.withColumn("customer_id", monotonically_increasing_id())
dim_seller = dim_seller.withColumn("seller_id", monotonically_increasing_id())
dim_product = dim_product.withColumn("product_id", monotonically_increasing_id())
dim_store = dim_store.withColumn("store_id", monotonically_increasing_id())
dim_supplier = dim_supplier.withColumn("supplier_id", monotonically_increasing_id())
dim_date = dim_date.withColumn("date_id", monotonically_increasing_id()).withColumn("year", year("date")).withColumn("month", month("date")).withColumn("day", dayofmonth("date"))

'''
fact_sales = (
    mock_df.alias("m")
    .join(dim_customer.alias("c"),
          col("m.customer_email") == col("c.customer_email"))
    .join(dim_seller.alias("s"),
          col("m.seller_email") == col("s.seller_email"))
    .join(dim_product.alias("p"),
          col("m.product_name") == col("p.product_name"))
    .join(dim_store.alias("st"),
          col("m.store_name") == col("st.store_name"))
    .join(dim_supplier.alias("sp"),
          col("m.supplier_name") == col("sp.supplier_name"))
    .join(dim_date.alias("d"),
          col("m.sale_date") == col("d.date"))
    .select(
        col("m.id").alias("sale_id"),
        col("c.customer_id"),
        col("s.seller_id"),
        col("p.product_id"),
        col("st.store_id"),
        col("sp.supplier_id"),
        col("d.date_id"),
        col("m.sale_quantity"),
        col("m.product_price"),
        col("m.sale_total_price")
    )
)
'''

print("---Preparing to write---")

dim_customer.write.jdbc(PG_URL, "dim_customer", "overwrite", PG_PROPS)
print("---dim_customer finished---")
dim_seller.write.jdbc(PG_URL, "dim_seller", "overwrite", PG_PROPS)
print("---dim_seller finished---")
dim_product.write.jdbc(PG_URL, "dim_product", "overwrite", PG_PROPS)
print("---dim_product finished---")
dim_store.write.jdbc(PG_URL, "dim_store", "overwrite", PG_PROPS)
print("---dim_store finished---")
dim_supplier.write.jdbc(PG_URL, "dim_supplier", "overwrite", PG_PROPS)
print("---dim_supplier finished---")
dim_date.write.jdbc(PG_URL, "dim_date", "overwrite", PG_PROPS)
print("---dim_date finished---")

print("---starting fact_sales---")

customer_dict = {row.customer_email: row.customer_id for row in dim_customer.collect()}
seller_dict = {row.seller_email: row.seller_id for row in dim_seller.collect()}
product_dict = {row.product_name: row.product_id for row in dim_product.collect()}
store_dict = {row.store_name: row.store_id for row in dim_store.collect()}
supplier_dict = {row.supplier_name: row.supplier_id for row in dim_supplier.collect()}
date_dict = {row.date: row.date_id for row in dim_date.collect()}

customer_udf = udf(lambda email: customer_dict.get(email), LongType())
seller_udf = udf(lambda email: seller_dict.get(email), LongType())
product_udf = udf(lambda name: product_dict.get(name), LongType())
store_udf = udf(lambda name: store_dict.get(name), LongType())
supplier_udf = udf(lambda name: supplier_dict.get(name), LongType())
date_udf = udf(lambda d: date_dict.get(d), LongType())

fact_sales = (
    mock_df
    .withColumn("customer_id", customer_udf(col("customer_email")))
    .withColumn("seller_id", seller_udf(col("seller_email")))
    .withColumn("product_id", product_udf(col("product_name")))
    .withColumn("store_id", store_udf(col("store_name")))
    .withColumn("supplier_id", supplier_udf(col("supplier_name")))
    .withColumn("date_id", date_udf(col("sale_date")))
    .select(
        col("id").alias("sale_id"),
        col("customer_id"),
        col("seller_id"),
        col("product_id"),
        col("store_id"),
        col("supplier_id"),
        col("date_id"),
        col("sale_quantity"),
        col("product_price"),
        col("sale_total_price")
    )
)

fact_sales.repartition(1).persist(StorageLevel.DISK_ONLY).write.option("batchsize", 500).jdbc(PG_URL, "fact_sales", "overwrite", PG_PROPS)
print("---fact_sales finished---")
spark.stop()
