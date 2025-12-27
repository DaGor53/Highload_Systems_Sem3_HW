import psycopg2
import os
from pathlib import Path
import time
import clickhouse_connect

time.sleep(20)

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "my_db")
DB_USER = os.getenv("DB_USER", "my_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "12345")

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

conn.autocommit = True
cur = conn.cursor()

def execute_sql_folder(folder_path: str):
    files = sorted(Path(folder_path).glob("*.sql"))
    for file in files:
        print(f"Executing {file}")
        with open(file, "r", encoding="utf-8") as f:
            cur.execute(f.read())

execute_sql_folder("/app/sql")

cur.close()
conn.close()

print("ALL SQLs executed!")

print("Modifying Clickhouse database...")
client = clickhouse_connect.get_client(
    host='clickhouse',
    port=8123,
    username='click',
    password='click',
    database='mydatabase'
)

tables = {
    "mart_product": """
        DROP TABLE IF EXISTS mart_product
    """,
    "mart_customer": """
        DROP TABLE IF EXISTS mart_customer
    """,
    "mart_time": """
        DROP TABLE IF EXISTS mart_time
    """,
    "mart_store": """
        DROP TABLE IF EXISTS mart_store
    """,
    "mart_supplier": """
        DROP TABLE IF EXISTS mart_supplier
    """,
    "mart_quality": """
        DROP TABLE IF EXISTS mart_quality
    """
}
for name, sql in tables.items():
    client.command(sql)

tables = {
    "mart_product": """
        CREATE TABLE IF NOT EXISTS mart_product
        (
            product_id Int64,
            product_name String,
            product_category String,
            revenue Float64,
            total_sales Int64,
            avg_rating Float32,
            avg_reviews Float32
        )
        ENGINE = MergeTree()
        ORDER BY product_id
    """,
    "mart_customer": """
        CREATE TABLE IF NOT EXISTS mart_customer
        (
            customer_id Int64,
            customer_country String,
            total_spent Float64,
            avg_check Float64
        )
        ENGINE = MergeTree()
        ORDER BY customer_id
    """,
    "mart_time": """
        CREATE TABLE IF NOT EXISTS mart_time
        (
            year Int16,
            month Int8,
            revenue Float64,
            avg_order_value Float64
        )
        ENGINE = MergeTree()
        ORDER BY (year, month)
    """,
    "mart_store": """
        CREATE TABLE IF NOT EXISTS mart_store
        (
            store_id Int64,
            store_city String,
            store_country String,
            revenue Float64,
            avg_check Float64
        )
        ENGINE = MergeTree()
        ORDER BY store_id
    """,
    "mart_supplier": """
        CREATE TABLE IF NOT EXISTS mart_supplier
        (
            supplier_id Int64,
            supplier_country String,
            revenue Float64,
            avg_price Float64
        )
        ENGINE = MergeTree()
        ORDER BY supplier_id
    """,
    "mart_quality": """
        CREATE TABLE IF NOT EXISTS mart_quality
        (
            product_id Int64,
            product_name String,
            avg_rating Float32,
            sales_volume Int64,
            avg_reviews Float32
        )
        ENGINE = MergeTree()
        ORDER BY product_id
    """
}

for name, sql in tables.items():
    client.command(sql)
    print(f"{name} created")

print("Praise The Lord and rejoice! Marts (Vitriny) for Clickhouse have been created!")