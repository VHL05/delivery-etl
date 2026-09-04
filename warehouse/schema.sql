-- 1. BẢNG DIMENSION
CREATE TABLE IF NOT EXISTS `delivery_dw.dim_customer` (
    customer_id STRING NOT NULL,
    customer_unique_id STRING,
    customer_zip_code_prefix STRING,
    customer_city STRING,
    customer_state STRING
);

CREATE TABLE IF NOT EXISTS `delivery_dw.dim_seller` (
    seller_id STRING NOT NULL,
    seller_zip_code_prefix STRING,
    seller_city STRING,
    seller_state STRING
);

CREATE TABLE IF NOT EXISTS `delivery_dw.dim_product` (
    product_id STRING NOT NULL,
    product_category_name STRING,
    product_name_lenght FLOAT64,
    product_description_lenght FLOAT64,
    product_photos_qty FLOAT64,
    product_weight_g FLOAT64,
    product_length_cm FLOAT64,
    product_height_cm FLOAT64,
    product_width_cm FLOAT64,
    product_volume FLOAT64
);

CREATE TABLE IF NOT EXISTS `delivery_dw.dim_location` (
    zip_code_prefix STRING NOT NULL,
    city STRING,
    state STRING,
    latitude FLOAT64,
    longitude FLOAT64
);

-- 2. BẢNG FACT
CREATE TABLE IF NOT EXISTS `delivery_dw.fact_delivery` (
    order_id STRING NOT NULL,
    customer_id STRING,
    product_id STRING,
    seller_id STRING,
    order_status STRING,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    price FLOAT64,
    freight_value FLOAT64,
    customer_zip_code_prefix STRING,
    delivery_duration_hours FLOAT64,
    delay_days INT64,
    is_delayed INT64,
    date_sk STRING
);