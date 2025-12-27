CREATE TABLE dim_product (
    product_id SERIAL PRIMARY KEY,
    name TEXT,
    category TEXT,
    pet_category TEXT,
    weight NUMERIC,
    color TEXT,
    size TEXT,
    brand TEXT,
    material TEXT,
    description TEXT,
    rating NUMERIC(3,2),
    reviews INT,
    release_date DATE,
    expiry_date DATE
);

INSERT INTO dim_product (
	name,
	category,
	pet_category,
	weight,
	color,
	size,
	brand,
	material,
	description,
	rating,
	reviews,
	release_date,
	expiry_date
)
SELECT DISTINCT
    product_name,
    product_category,
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
FROM mock_data;