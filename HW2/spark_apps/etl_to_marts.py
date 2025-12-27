from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, col, desc, udf
from pyspark.sql.types import LongType, FloatType, StringType

spark = (SparkSession.builder.appName("etl_to_marts").getOrCreate())

PG_URL = "jdbc:postgresql://postgres:5432/my_db"
PG_PROPS = {
    "user": "my_user",
    "password": "12345",
    "driver": "org.postgresql.Driver"
}

CH_URL = "jdbc:clickhouse://clickhouse:8123/mydatabase"
CH_PROPS = {
    "user": "click",
    "password": "click",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

fact = spark.read.jdbc(PG_URL, "fact_sales", properties=PG_PROPS)
product = spark.read.jdbc(PG_URL, "dim_product", properties=PG_PROPS)
customer = spark.read.jdbc(PG_URL, "dim_customer", properties=PG_PROPS)
store = spark.read.jdbc(PG_URL, "dim_store", properties=PG_PROPS)
supplier = spark.read.jdbc(PG_URL, "dim_supplier", properties=PG_PROPS)
date = spark.read.jdbc(PG_URL, "dim_date", properties=PG_PROPS)

customer_dict = {row.customer_id: row.customer_country for row in customer.collect()}
product_dict = {row.product_id: (row.product_name, row.product_category, row.product_rating, row.product_reviews) for row in product.collect()}
store_dict = {row.store_id: (row.store_city, row.store_country) for row in store.collect()}
supplier_dict = {row.supplier_id: row.supplier_country for row in supplier.collect()}
date_dict = {row.date_id: (row.year, row.month) for row in date.collect()}

get_customer_country = udf(lambda cid: customer_dict.get(cid), StringType())
get_product_name = udf(lambda pid: product_dict.get(pid)[0], StringType())
get_product_category = udf(lambda pid: product_dict.get(pid)[1], StringType())
get_product_rating = udf(lambda pid: float(product_dict.get(pid)[2]), FloatType())
get_product_reviews = udf(lambda pid: float(product_dict.get(pid)[3]), FloatType())
get_store_city = udf(lambda sid: store_dict.get(sid)[0], StringType())
get_store_country = udf(lambda sid: store_dict.get(sid)[1], StringType())
get_supplier_country = udf(lambda sid: supplier_dict.get(sid), StringType())
get_year = udf(lambda did: date_dict.get(did)[0], LongType())
get_month = udf(lambda did: date_dict.get(did)[1], LongType())

mart_product = (
    fact.withColumn("product_name", get_product_name(col("product_id")))
        .withColumn("product_category", get_product_category(col("product_id")))
        .withColumn("product_rating_val", get_product_rating(col("product_id")))
        .withColumn("product_reviews_val", get_product_reviews(col("product_id")))
        .groupBy("product_id", "product_name", "product_category")
        .agg(
            sum("sale_total_price").alias("revenue"),
            sum("sale_quantity").alias("total_sales"),
            avg("product_rating_val").alias("avg_rating"),
            avg("product_reviews_val").alias("avg_reviews")
        )
)

mart_customer = (
    fact.withColumn("customer_country", get_customer_country(col("customer_id")))
        .groupBy("customer_id", "customer_country")
        .agg(
            sum("sale_total_price").alias("total_spent"),
            avg("sale_total_price").alias("avg_check")
        )
)

mart_time = (
    fact.withColumn("year", get_year(col("date_id")))
        .withColumn("month", get_month(col("date_id")))
        .groupBy("year", "month")
        .agg(
            sum("sale_total_price").alias("revenue"),
            avg("sale_total_price").alias("avg_order_value")
        )
)

mart_store = (
    fact.withColumn("store_city", get_store_city(col("store_id")))
        .withColumn("store_country", get_store_country(col("store_id")))
        .groupBy("store_id", "store_city", "store_country")
        .agg(
            sum("sale_total_price").alias("revenue"),
            avg("sale_total_price").alias("avg_check")
        )
)

mart_supplier = (
    fact.withColumn("supplier_country", get_supplier_country(col("supplier_id")))
        .groupBy("supplier_id", "supplier_country")
        .agg(
            sum("sale_total_price").alias("revenue"),
            avg("product_price").alias("avg_price")
        )
)

mart_quality = (
    fact.withColumn("product_name", get_product_name(col("product_id")))
        .withColumn("product_rating_val", get_product_rating(col("product_id")))
        .withColumn("product_reviews_val", get_product_reviews(col("product_id")))
        .groupBy("product_id", "product_name")
        .agg(
            avg("product_rating_val").alias("avg_rating"),
            count("sale_id").alias("sales_volume"),
            avg("product_reviews_val").alias("avg_reviews")
        )
)


mart_product = (
    mart_product
    .withColumn("product_id", col("product_id").cast(LongType()))
    .withColumn("revenue", col("revenue").cast(FloatType()))
    .withColumn("total_sales", col("total_sales").cast(LongType()))
    .withColumn("avg_rating", col("avg_rating").cast(FloatType()))
    .withColumn("avg_reviews", col("avg_reviews").cast(FloatType()))
    .withColumn("product_name", col("product_name").cast(StringType()))
    .withColumn("product_category", col("product_category").cast(StringType()))
)

mart_customer = (
    mart_customer
    .withColumn("customer_id", col("customer_id").cast(LongType()))
    .withColumn("total_spent", col("total_spent").cast(FloatType()))
    .withColumn("avg_check", col("avg_check").cast(FloatType()))
    .withColumn("customer_country", col("customer_country").cast(StringType()))
)

mart_time = (
    mart_time
    .withColumn("year", col("year").cast(LongType()))
    .withColumn("month", col("month").cast(LongType()))
    .withColumn("revenue", col("revenue").cast(FloatType()))
    .withColumn("avg_order_value", col("avg_order_value").cast(FloatType()))
)

mart_store = (
    mart_store
    .withColumn("store_id", col("store_id").cast(LongType()))
    .withColumn("revenue", col("revenue").cast(FloatType()))
    .withColumn("avg_check", col("avg_check").cast(FloatType()))
    .withColumn("store_city", col("store_city").cast(StringType()))
    .withColumn("store_country", col("store_country").cast(StringType()))
)

mart_supplier = (
    mart_supplier
    .withColumn("supplier_id", col("supplier_id").cast(LongType()))
    .withColumn("revenue", col("revenue").cast(FloatType()))
    .withColumn("avg_price", col("avg_price").cast(FloatType()))
    .withColumn("supplier_country", col("supplier_country").cast(StringType()))
)

mart_quality = (
    mart_quality
    .withColumn("product_id", col("product_id").cast(LongType()))
    .withColumn("avg_rating", col("avg_rating").cast(FloatType()))
    .withColumn("sales_volume", col("sales_volume").cast(LongType()))
    .withColumn("avg_reviews", col("avg_reviews").cast(FloatType()))
    .withColumn("product_name", col("product_name").cast(StringType()))
)

mart_product.write.jdbc(CH_URL, "mart_product", "append", CH_PROPS)
mart_customer.write.jdbc(CH_URL, "mart_customer", "append", CH_PROPS)
mart_time.write.jdbc(CH_URL, "mart_time", "append", CH_PROPS)
mart_store.write.jdbc(CH_URL, "mart_store", "append", CH_PROPS)
mart_supplier.write.jdbc(CH_URL, "mart_supplier", "append", CH_PROPS)
mart_quality.write.jdbc(CH_URL, "mart_quality", "append", CH_PROPS)

print("All marts successfully loaded into ClickHouse!")
