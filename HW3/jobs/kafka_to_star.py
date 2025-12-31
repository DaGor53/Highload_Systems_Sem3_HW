
from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

t_env.execute_sql("""
CREATE TABLE kafka_sales (
    customer_first_name STRING,
    customer_last_name STRING,
    customer_age INT,
    customer_email STRING,
    customer_country STRING,
    customer_postal_code STRING,
    customer_pet_type STRING,
    customer_pet_name STRING,
    customer_pet_breed STRING,
    seller_first_name STRING,
    seller_last_name STRING,
    seller_email STRING,
    seller_country STRING,
    seller_postal_code STRING,
    product_name STRING,
    product_category STRING,
    product_price DECIMAL(10,2),
    pet_category STRING,
    product_weight DECIMAL(10,2),
    product_color STRING,
    product_size STRING,
    product_brand STRING,
    product_material STRING,
    product_description STRING,
    product_rating DECIMAL(3,2),
    product_reviews INT,
    product_release_date STRING,
    product_expiry_date STRING,
    store_name STRING,
    store_location STRING,
    store_city STRING,
    store_state STRING,
    store_country STRING,
    store_phone STRING,
    store_email STRING,
    supplier_name STRING,
    supplier_contact STRING,
    supplier_email STRING,
    supplier_phone STRING,
    supplier_address STRING,
    supplier_city STRING,
    supplier_country STRING,
    sale_quantity INT,
    sale_total_price DECIMAL(10,2),
    sale_date STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'sales_stream',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-sales-group',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
)
""")

t_env.execute_sql("""
CREATE TABLE dim_customer_sink (
    customer_first_name STRING,
    customer_last_name STRING,
    customer_age INT,
    customer_email STRING,
    customer_country STRING,
    customer_postal_code STRING,
    customer_pet_type STRING,
    customer_pet_name STRING,
    customer_pet_breed STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/my_db',
    'table-name' = 'dim_customer',
    'username' = 'my_user',
    'password' = '12345',
    'driver' = 'org.postgresql.Driver'
)
""")

t_env.execute_sql("""
CREATE TABLE dim_seller_sink (
    seller_first_name STRING,
    seller_last_name STRING,
    seller_email STRING,
    seller_country STRING,
    seller_postal_code STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/my_db',
    'table-name' = 'dim_seller',
    'username' = 'my_user',
    'password' = '12345',
    'driver' = 'org.postgresql.Driver'
)
""")

t_env.execute_sql("""
CREATE TABLE dim_product_sink (
    product_name STRING,
    product_category STRING,
    product_price DECIMAL(10,2),
    product_pet_category STRING,
    product_weight DECIMAL(10,2),
    product_color STRING,
    product_size STRING,
    product_brand STRING,
    product_material STRING,
    product_description STRING,
    product_rating DECIMAL(3,2),
    product_reviews INT,
    product_release_date STRING,
    product_expiry_date STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/my_db',
    'table-name' = 'dim_product',
    'username' = 'my_user',
    'password' = '12345',
    'driver' = 'org.postgresql.Driver'
)
""")

t_env.execute_sql("""
CREATE TABLE dim_store_sink (
    store_name STRING,
    store_location STRING,
    store_city STRING,
    store_state STRING,
    store_country STRING,
    store_phone STRING,
    store_email STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/my_db',
    'table-name' = 'dim_store',
    'username' = 'my_user',
    'password' = '12345',
    'driver' = 'org.postgresql.Driver'
)
""")

t_env.execute_sql("""
CREATE TABLE dim_supplier_sink (
    supplier_name STRING,
    supplier_contact STRING,
    supplier_email STRING,
    supplier_phone STRING,
    supplier_address STRING,
    supplier_city STRING,
    supplier_country STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/my_db',
    'table-name' = 'dim_supplier',
    'username' = 'my_user',
    'password' = '12345',
    'driver' = 'org.postgresql.Driver'
)
""")

t_env.execute_sql("""
INSERT INTO dim_customer_sink
SELECT
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country,
    customer_postal_code,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed
FROM kafka_sales
""")

t_env.execute_sql("""
INSERT INTO dim_seller_sink
SELECT
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
FROM kafka_sales
""")

t_env.execute_sql("""
INSERT INTO dim_product_sink
SELECT
    product_name,
    product_category,
    product_price,
    pet_category,
    product_weight,
    product_color,
    product_size,
    product_brand,
    product_material,
    product_description,
    product_rating,
    product_reviews,
    product_release_date,
    product_expiry_date
FROM kafka_sales
""")

t_env.execute_sql("""
INSERT INTO dim_store_sink
SELECT
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
FROM kafka_sales
""")

t_env.execute_sql("""
INSERT INTO dim_supplier_sink
SELECT
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM kafka_sales
""")
# ------------------ Fact sales ------------------
t_env.execute_sql("""
CREATE TABLE fact_sales_sink (
    customer_email STRING,
    seller_email STRING,
    product_name STRING,
    store_email STRING,
    supplier_email STRING,
    sale_date STRING,
    sale_quantity INT,
    sale_total_price DECIMAL(10,2)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/my_db',
    'table-name' = 'raw_fact_sales',
    'username' = 'my_user',
    'password' = '12345',
    'driver' = 'org.postgresql.Driver'
)
""")

t_env.execute_sql("""
INSERT INTO fact_sales_sink
SELECT
    customer_email,
    seller_email,
    product_name,
    store_email,
    supplier_email,
    sale_date,
    sale_quantity,
    sale_total_price
FROM kafka_sales
""")