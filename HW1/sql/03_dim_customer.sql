CREATE TABLE dim_customer (
    customer_id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    age INT,
    email TEXT UNIQUE,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

INSERT INTO dim_customer (
    first_name,
    last_name,
    age,
    email,
    country,
    postal_code,
    pet_type,
    pet_name,
    pet_breed
)
SELECT DISTINCT
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country,
    customer_postal_code,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed
FROM mock_data;