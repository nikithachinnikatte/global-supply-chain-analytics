#setup_db
from sqlalchemy import create_engine, text

# 1) PASTE YOUR NEON CONNECTION STRING HERE (IN THIS EXACT FORMAT)
NEON_URL = "postgresql+psycopg2://neondb_owner:npg_H3BTFGQIzNL9@ep-lingering-darkness-a13w1fg0-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"

engine = create_engine(NEON_URL)

create_sql = """
CREATE TABLE IF NOT EXISTS dim_product (
    product_id SERIAL PRIMARY KEY,
    product_sku VARCHAR(100),
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255),
    segment VARCHAR(50),
    city VARCHAR(100),
    country VARCHAR(100),
    region VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(255),
    supplier_country VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    month_name VARCHAR(20),
    quarter INT
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id VARCHAR(100) PRIMARY KEY,
    order_date_id DATE REFERENCES dim_date(date_id),
    ship_date_id DATE REFERENCES dim_date(date_id),
    customer_id INT REFERENCES dim_customer(customer_id),
    product_id INT REFERENCES dim_product(product_id),
    supplier_id INT REFERENCES dim_supplier(supplier_id),
    quantity INT,
    unit_price NUMERIC(10,2),
    shipping_cost NUMERIC(10,2),
    total_revenue NUMERIC(12,2),
    profit NUMERIC(12,2),
    lead_time_days INT,
    on_time_flag BOOLEAN
);
"""

with engine.begin() as conn:
    conn.execute(text(create_sql))

print("✅ Tables created (or already existed) in Neon.")
#setup_db

