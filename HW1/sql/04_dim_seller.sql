CREATE TABLE dim_seller (
    seller_id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT UNIQUE,
    country TEXT,
    postal_code TEXT
);

INSERT INTO dim_seller (
	first_name,
	last_name,
	email,
	country,
	postal_code
)
SELECT DISTINCT
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
FROM mock_data;