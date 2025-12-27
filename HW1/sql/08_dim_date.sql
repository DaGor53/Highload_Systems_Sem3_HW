CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE UNIQUE,
    year INT,
    month INT,
    day INT
);

INSERT INTO dim_date (
	date,
	year,
	month,
	day
)
SELECT DISTINCT
    sale_date,
    EXTRACT(YEAR FROM sale_date),
    EXTRACT(MONTH FROM sale_date),
    EXTRACT(DAY FROM sale_date)
FROM mock_data;