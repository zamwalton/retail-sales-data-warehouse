-- dim_products.sql
-- Product dimension with performance metrics per product.
-- Grain: one row per product_id

{{
  config(
    materialized = 'table',
    tags         = ['marts', 'dimension']
  )
}}

WITH orders AS (

    SELECT * FROM {{ ref('stg_orders') }}

),

category_mode AS (
    SELECT product_id, product_category,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY COUNT(*) DESC) AS rn
    FROM orders
    WHERE product_category IS NOT NULL
    GROUP BY product_id, product_category
),

product_metrics AS (
    SELECT
        o.product_id,
        cat.product_category,
        COUNT(DISTINCT o.order_id)              AS total_orders,
        SUM(o.quantity)                         AS total_units_sold,
        SUM(o.net_amount)                       AS total_revenue,
        AVG(o.unit_price)                       AS avg_selling_price,
        AVG(o.discount_pct)                     AS avg_discount_pct,
        SUM(CASE WHEN o.order_status = 'returned' THEN 1 ELSE 0 END) AS return_count,
        COUNT(DISTINCT o.customer_id)           AS unique_buyers
    FROM orders o
    LEFT JOIN category_mode cat ON o.product_id = cat.product_id AND cat.rn = 1
    GROUP BY o.product_id, cat.product_category
),

ranked AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['product_id']) }}
                                                AS product_key,
        product_id,
        product_category                        AS category,
        total_orders,
        total_units_sold,
        ROUND(total_revenue, 2)                 AS total_revenue,
        ROUND(avg_selling_price, 2)             AS avg_selling_price,
        ROUND(avg_discount_pct, 1)              AS avg_discount_pct,
        return_count,
        unique_buyers,

        -- Return rate
        ROUND(return_count * 100.0 / NULLIF(total_orders, 0), 2)
                                                AS return_rate_pct,

        -- Product tier by revenue
        CASE
            WHEN RANK() OVER (ORDER BY total_revenue DESC) <= 100
                THEN 'top_100'
            WHEN RANK() OVER (ORDER BY total_revenue DESC) <= 500
                THEN 'top_500'
            ELSE 'standard'
        END                                     AS revenue_tier,

        CURRENT_TIMESTAMP()                     AS dbt_updated_at

    FROM product_metrics

)

SELECT * FROM ranked
