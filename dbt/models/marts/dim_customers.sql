-- dim_customers.sql
-- Customer dimension with RFM-based segmentation.
-- Grain: one row per customer_id

{{
  config(
    materialized = 'table',
    tags         = ['marts', 'dimension']
  )
}}

WITH orders AS (

    SELECT * FROM {{ ref('stg_orders') }}

),

country_mode AS (
    SELECT customer_id, country_code,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC) AS rn
    FROM orders
    WHERE country_code IS NOT NULL
    GROUP BY customer_id, country_code
),

customer_metrics AS (
    SELECT
        o.customer_id,
        cm.country_code,
        MIN(o.order_date)                       AS first_order_date,
        MAX(o.order_date)                       AS last_order_date,
        COUNT(DISTINCT o.order_id)              AS total_orders,
        SUM(o.net_amount)                       AS total_spend,
        AVG(o.net_amount)                       AS avg_order_value,
        COUNT(DISTINCT o.product_category)      AS distinct_categories,
        DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) AS days_since_last_order
    FROM orders o
    LEFT JOIN country_mode cm ON o.customer_id = cm.customer_id AND cm.rn = 1
    WHERE o.order_status != 'cancelled'
    GROUP BY o.customer_id, cm.country_code
),

segmented AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }}
                                            AS customer_key,
        customer_id,
        country_code,
        first_order_date,
        last_order_date,
        total_orders,
        ROUND(total_spend, 2)               AS total_spend,
        ROUND(avg_order_value, 2)           AS avg_order_value,
        distinct_categories,
        days_since_last_order,

        -- RFM Segmentation (Recency, Frequency, Monetary)
        CASE
            WHEN total_spend     >= 10000 AND total_orders >= 20
                THEN 'platinum'
            WHEN total_spend     >= 5000  AND total_orders >= 10
                THEN 'gold'
            WHEN total_spend     >= 1000  AND total_orders >= 3
                THEN 'silver'
            ELSE 'bronze'
        END                                 AS customer_segment,

        -- Churn risk flag
        CASE
            WHEN days_since_last_order > 180 THEN 'high'
            WHEN days_since_last_order > 90  THEN 'medium'
            ELSE 'low'
        END                                 AS churn_risk,

        CURRENT_TIMESTAMP()                 AS dbt_updated_at

    FROM customer_metrics

)

SELECT * FROM segmented
