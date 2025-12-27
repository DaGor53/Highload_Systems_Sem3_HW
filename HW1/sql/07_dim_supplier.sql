CREATE TABLE dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    name TEXT,
    contact TEXT,
    email TEXT UNIQUE,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT
);

INSERT INTO dim_supplier (
	name,
	contact,
	email,
	phone,
	address,
	city,
	country
)
SELECT DISTINCT
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM mock_data;