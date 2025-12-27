import psycopg2
import os
from pathlib import Path
import time

time.sleep(10)

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

print("Creating tables and loading data...")
execute_sql_folder("/app/sql")

cur.close()
conn.close()

print("ALL SQLs executed!")