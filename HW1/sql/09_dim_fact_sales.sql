CREATE INDEX idx_customer_email  ON dim_customer(email);
CREATE INDEX idx_seller_email    ON dim_seller(email);
CREATE INDEX idx_product_name    ON dim_product(name);
CREATE INDEX idx_store_name      ON dim_store(name);
CREATE INDEX idx_supplier_name   ON dim_supplier(name);
CREATE INDEX idx_date_date       ON dim_date(date);

CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES dim_customer(customer_id),
    seller_id INT REFERENCES dim_seller(seller_id),
    product_id INT REFERENCES dim_product(product_id),
    store_id INT REFERENCES dim_store(store_id),
    supplier_id INT REFERENCES dim_supplier(supplier_id),
    date_id INT REFERENCES dim_date(date_id),
    sale_quantity INT,
    product_price NUMERIC(10,2),
    sale_total_price NUMERIC(10,2)
);

INSERT INTO fact_sales (
	customer_id,
	seller_id,
	product_id,
	store_id,
	supplier_id,
	date_id,
	sale_quantity,
	product_price,
	sale_total_price
)
SELECT
    c.customer_id,
    s.seller_id,
    p.product_id,
    st.store_id,
    sp.supplier_id,
    d.date_id,
    m.sale_quantity,
    m.product_price,
    m.sale_total_price
FROM mock_data m
JOIN dim_customer c  ON c.email = m.customer_email
JOIN dim_seller s    ON s.email = m.seller_email
JOIN dim_product p   ON p.name = m.product_name
JOIN dim_store st    ON st.name = m.store_name
JOIN dim_supplier sp ON sp.name = m.supplier_name
JOIN dim_date d      ON d.date = m.sale_date;