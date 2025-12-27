CREATE TABLE dim_store (
    store_id SERIAL PRIMARY KEY,
    name TEXT,
    location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    phone TEXT,
    email TEXT UNIQUE
);

INSERT INTO dim_store (
	name,
	location,
	city,
	state,
	country,
	phone,
	email
)
SELECT DISTINCT
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
FROM mock_data;