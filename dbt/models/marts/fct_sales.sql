-- fct_sales.sql
-- Core fact table: one row per order, fully enriched with dimension keys.
-- Grain: order_id (unique per row)
-- Materialized as TABLE — optimised for BI tool queries.

{{
  config(
    materialized    = 'table',
    cluster_by      = ['order_year', 'order_month'],
    tags            = ['marts', 'fact', 'daily']
  )
}}

WITH orders AS (

    SELECT * FROM {{ ref('stg_orders') }}

),

customers AS (

    SELECT customer_id, customer_segment
    FROM {{ ref('dim_customers') }}

),

products AS (

    SELECT product_id, category AS product_category_clean
    FROM {{ ref('dim_products') }}

),

joined AS (

    SELECT
        -- ── Surrogate Key ─────────────────────────────────────────────────────
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }}
                                                AS sale_key,

        -- ── Dimension FKs ────────────────────────────────────────────────────
        o.order_id,
        o.customer_id,
        o.product_id,
        o.store_id,

        -- ── Date Key (YYYYMMDD integer for join to dim_date) ──────────────────
        TO_NUMBER(TO_CHAR(o.order_date, 'YYYYMMDD'))
                                                AS date_key,

        -- ── Order Descriptors ─────────────────────────────────────────────────
        o.sales_channel,
        o.payment_method,
        o.order_status,
        o.country_code,
        COALESCE(c.customer_segment, 'unknown') AS customer_segment,

        -- ── Measures ─────────────────────────────────────────────────────────
        o.quantity,
        o.unit_price,
        o.discount_pct,
        COALESCE(o.discount_amount, 0)                                          AS discount_amount,
        o.gross_amount,
        COALESCE(o.net_amount, o.gross_amount)                                  AS net_amount,
        o.days_to_ship,

        -- ── Calendar Helpers ──────────────────────────────────────────────────
        o.order_date,
        o.ship_date,
        o.order_year,
        o.order_month,
        o.order_quarter,
        o.day_of_week,
        o.is_weekend,

        -- ── Flags ─────────────────────────────────────────────────────────────
        CASE WHEN o.order_status = 'returned' THEN TRUE ELSE FALSE END
                                                AS is_return,
        CASE WHEN o.discount_pct > 0          THEN TRUE ELSE FALSE END
                                                AS has_discount,
        CASE WHEN o.days_to_ship <= 2          THEN TRUE ELSE FALSE END
                                                AS is_fast_shipped,

        -- ── Metadata ─────────────────────────────────────────────────────────
        o.etl_loaded_at,
        CURRENT_TIMESTAMP()                     AS dbt_updated_at

    FROM orders     o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    LEFT JOIN products  p ON o.product_id  = p.product_id

)

SELECT * FROM joined
