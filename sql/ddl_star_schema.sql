-- Tạo schema để quản lý tầng Gold
CREATE SCHEMA IF NOT EXISTS public;

-- Định nghĩa bảng Dimension Date
CREATE TABLE IF NOT EXISTS public.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    quarter INT,
    day_of_week VARCHAR(20)
);

-- Định nghĩa bảng Dimension Products
CREATE TABLE IF NOT EXISTS public.dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    category_name VARCHAR(100),
    product_weight_g FLOAT
);

-- Định nghĩa bảng Fact Sales
CREATE TABLE IF NOT EXISTS public.fact_sales (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    date_key INT,
    price FLOAT,
    freight_value FLOAT,
    delivery_days INT,
    CONSTRAINT fk_date FOREIGN KEY(date_key) REFERENCES public.dim_date(date_key),
    CONSTRAINT fk_product FOREIGN KEY(product_id) REFERENCES public.dim_products(product_id)
);