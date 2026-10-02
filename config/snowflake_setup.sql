-- ═══════════════════════════════════════════════════════════════════════════
-- SNOWFLAKE SETUP — Retail Sales Intelligence Pipeline
-- Run this entire script in a Snowflake Worksheet as SYSADMIN / ACCOUNTADMIN
-- ═══════════════════════════════════════════════════════════════════════════

-- ── 1. Warehouse ────────────────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS RETAIL_WH
    WAREHOUSE_SIZE      = 'X-SMALL'
    AUTO_SUSPEND        = 60          -- suspend after 60s idle (cost control)
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT             = 'Retail pipeline compute — auto-suspend 60s';

-- ── 2. Database & Schemas ────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS RETAIL_DB;

CREATE SCHEMA IF NOT EXISTS RETAIL_DB.RAW
    COMMENT = 'Bronze layer: raw ingested data, unchanged from source';

CREATE SCHEMA IF NOT EXISTS RETAIL_DB.STAGING
    COMMENT = 'Silver layer: cleaned and typed data, dbt staging models';

CREATE SCHEMA IF NOT EXISTS RETAIL_DB.MARTS
    COMMENT = 'Gold layer: star schema fact/dim tables for BI consumption';

-- ── 3. RAW Layer — Orders ────────────────────────────────────────────────────
USE SCHEMA RETAIL_DB.RAW;

CREATE OR REPLACE TABLE orders (
    order_id        VARCHAR(36)   NOT NULL,
    customer_id     VARCHAR(10)   NOT NULL,
    product_id      VARCHAR(10)   NOT NULL,
    store_id        VARCHAR(10),
    category        VARCHAR(50),
    quantity        INTEGER,
    unit_price      FLOAT,
    discount_pct    INTEGER,
    order_date      DATE,
    ship_date       DATE,
    status          VARCHAR(20),
    country         VARCHAR(5),
    channel         VARCHAR(20),
    payment         VARCHAR(20),
    gross_amount    FLOAT,
    net_amount      FLOAT,
    discount_amount FLOAT,
    year            INTEGER,
    month           INTEGER,
    quarter         INTEGER,
    day_of_week     INTEGER,
    is_weekend      BOOLEAN,
    days_to_ship    INTEGER,
    etl_loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_orders PRIMARY KEY (order_id)
);

-- ── 4. S3 External Stage ─────────────────────────────────────────────────────
-- Replace with your actual S3 bucket and IAM role for production
CREATE OR REPLACE STAGE RETAIL_DB.RAW.orders_stage
    URL         = 's3://your-actual-bucket-name/staging/orders/'
    CREDENTIALS = (AWS_KEY_ID = 'your_real_key' AWS_SECRET_KEY = 'your_real_secret')
    FILE_FORMAT = (TYPE = 'PARQUET');

-- ── 5. Load Data via COPY INTO ───────────────────────────────────────────────
COPY INTO RETAIL_DB.RAW.orders (
    order_id, customer_id, product_id, store_id, category,
    quantity, unit_price, discount_pct, order_date, ship_date,
    status, country, channel, payment,
    gross_amount, net_amount, discount_amount,
    year, month, quarter, day_of_week, is_weekend, days_to_ship
)
FROM (
    SELECT
        $1:order_id::VARCHAR,
        $1:customer_id::VARCHAR,
        $1:product_id::VARCHAR,
        $1:store_id::VARCHAR,
        $1:category::VARCHAR,
        $1:quantity::INTEGER,
        $1:unit_price::FLOAT,
        $1:discount_pct::INTEGER,
        $1:order_date::DATE,
        $1:ship_date::DATE,
        $1:status::VARCHAR,
        $1:country::VARCHAR,
        $1:channel::VARCHAR,
        $1:payment::VARCHAR,
        $1:gross_amount::FLOAT,
        $1:net_amount::FLOAT,
        $1:discount_amount::FLOAT,
        $1:year::INTEGER,
        $1:month::INTEGER,
        $1:quarter::INTEGER,
        $1:day_of_week::INTEGER,
        $1:is_weekend::BOOLEAN,
        $1:days_to_ship::INTEGER
    FROM @RETAIL_DB.RAW.orders_stage
)
ON_ERROR = 'CONTINUE'
PURGE    = FALSE;

-- ── 6. Verify Load ───────────────────────────────────────────────────────────
SELECT
    COUNT(*)                                      AS total_orders,
    COUNT(DISTINCT customer_id)                   AS unique_customers,
    COUNT(DISTINCT product_id)                    AS unique_products,
    MIN(order_date)                               AS earliest_order,
    MAX(order_date)                               AS latest_order,
    ROUND(SUM(net_amount) / 1e6, 2)              AS total_revenue_m,
    ROUND(AVG(net_amount), 2)                     AS avg_order_value
FROM RETAIL_DB.RAW.orders;

-- ── 7. MARTS Schema — Star Schema Tables ────────────────────────────────────
-- These are managed by dbt, but pre-created so Snowflake permissions are set

USE SCHEMA RETAIL_DB.MARTS;

-- Dimension: Customers
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key     INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_id      VARCHAR(10) NOT NULL UNIQUE,
    country          VARCHAR(5),
    first_order_date DATE,
    last_order_date  DATE,
    total_orders     INTEGER,
    total_spend      FLOAT,
    customer_segment VARCHAR(20),  -- bronze / silver / gold
    dbt_updated_at   TIMESTAMP_NTZ
);

-- Dimension: Products
CREATE TABLE IF NOT EXISTS dim_products (
    product_key  INTEGER AUTOINCREMENT PRIMARY KEY,
    product_id   VARCHAR(10) NOT NULL UNIQUE,
    category     VARCHAR(50),
    avg_price    FLOAT,
    dbt_updated_at TIMESTAMP_NTZ
);

-- Dimension: Date (pre-populated by dbt seed)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key     INTEGER PRIMARY KEY,   -- YYYYMMDD
    full_date    DATE,
    year         INTEGER,
    quarter      INTEGER,
    month        INTEGER,
    month_name   VARCHAR(10),
    week         INTEGER,
    day_of_week  INTEGER,
    day_name     VARCHAR(10),
    is_weekend   BOOLEAN,
    is_holiday   BOOLEAN
);

-- Fact: Sales
CREATE TABLE IF NOT EXISTS fct_sales (
    sale_key         INTEGER AUTOINCREMENT PRIMARY KEY,
    order_id         VARCHAR(36) NOT NULL,
    customer_key     INTEGER REFERENCES dim_customers(customer_key),
    product_key      INTEGER REFERENCES dim_products(product_key),
    date_key         INTEGER REFERENCES dim_date(date_key),
    store_id         VARCHAR(10),
    channel          VARCHAR(20),
    payment          VARCHAR(20),
    status           VARCHAR(20),
    quantity         INTEGER,
    unit_price       FLOAT,
    discount_pct     INTEGER,
    discount_amount  FLOAT,
    gross_amount     FLOAT,
    net_amount       FLOAT,
    days_to_ship     INTEGER,
    dbt_updated_at   TIMESTAMP_NTZ
);

-- ── 8. Role & Access Setup (production best practice) ────────────────────────
CREATE ROLE IF NOT EXISTS RETAIL_ENGINEER;
CREATE ROLE IF NOT EXISTS RETAIL_ANALYST;

GRANT USAGE ON WAREHOUSE RETAIL_WH      TO ROLE RETAIL_ENGINEER;
GRANT USAGE ON DATABASE  RETAIL_DB      TO ROLE RETAIL_ENGINEER;
GRANT ALL   ON ALL SCHEMAS IN DATABASE RETAIL_DB TO ROLE RETAIL_ENGINEER;

GRANT USAGE ON WAREHOUSE RETAIL_WH      TO ROLE RETAIL_ANALYST;
GRANT USAGE ON DATABASE  RETAIL_DB      TO ROLE RETAIL_ANALYST;
GRANT USAGE ON SCHEMA RETAIL_DB.MARTS  TO ROLE RETAIL_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA RETAIL_DB.MARTS TO ROLE RETAIL_ANALYST;

SELECT 'Setup complete. Retail pipeline Snowflake objects created.' AS status;
