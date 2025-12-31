CREATE TABLE dim_customer (
    customer_id SERIAL PRIMARY KEY,
    customer_first_name TEXT,
    customer_last_name TEXT,
    customer_age INT,
    customer_email TEXT,
    customer_country TEXT,
    customer_postal_code TEXT,
    customer_pet_type TEXT,
    customer_pet_name TEXT,
    customer_pet_breed TEXT
);

CREATE TABLE dim_seller (
    seller_id SERIAL PRIMARY KEY,
    seller_first_name TEXT,
    seller_last_name TEXT,
    seller_email TEXT UNIQUE,
    seller_country TEXT,
    seller_postal_code TEXT
);

CREATE TABLE dim_product (
    product_id SERIAL PRIMARY KEY,
    product_name TEXT,
    product_category TEXT,
	product_price NUMERIC(10,2),
    product_pet_category TEXT,
    product_weight NUMERIC,
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating NUMERIC(3,2),
    product_reviews INT,
    product_release_date TEXT,
    product_expiry_date TEXT
);

CREATE TABLE dim_store (
    store_id SERIAL PRIMARY KEY,
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT UNIQUE
);

CREATE TABLE dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT UNIQUE,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);

CREATE TABLE raw_fact_sales (
    sale_id SERIAL PRIMARY KEY,
    customer_email      TEXT,
    seller_email        TEXT,
    product_name        TEXT,
    store_email         TEXT,
    supplier_email      TEXT,
    sale_date           TEXT,
    sale_quantity       INT,
    sale_total_price    NUMERIC(10,2)
);