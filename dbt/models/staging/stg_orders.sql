-- stg_orders.sql
-- Staging model: clean, typed, renamed columns from RAW.orders
-- Materialized as VIEW — always reads freshest data, zero storage cost.

{{
  config(
    materialized = 'view',
    tags         = ['staging', 'orders']
  )
}}

WITH source AS (

    SELECT * FROM {{ source('raw', 'orders') }}

),

renamed AS (

    SELECT
        -- ── Keys ─────────────────────────────────────────────────────────────
        order_id                                    AS order_id,
        customer_id                                 AS customer_id,
        product_id                                  AS product_id,
        store_id                                    AS store_id,

        -- ── Order Attributes ─────────────────────────────────────────────────
        category                                    AS product_category,
        LOWER(TRIM(channel))                        AS sales_channel,
        LOWER(TRIM(payment))                        AS payment_method,
        LOWER(TRIM(status))                         AS order_status,
        UPPER(TRIM(country))                        AS country_code,

        -- ── Numeric Columns ───────────────────────────────────────────────────
        quantity::INTEGER                           AS quantity,
        unit_price::FLOAT                           AS unit_price,
        COALESCE(discount_pct, 0)::INTEGER          AS discount_pct,
        gross_amount::FLOAT                         AS gross_amount,
        discount_amount::FLOAT                      AS discount_amount,
        net_amount::FLOAT                           AS net_amount,

        -- ── Date Columns ──────────────────────────────────────────────────────
        order_date::DATE                            AS order_date,
        ship_date::DATE                             AS ship_date,
        days_to_ship::INTEGER                       AS days_to_ship,

        -- ── Calendar Dimensions ──────────────────────────────────────────────
        year::INTEGER                               AS order_year,
        month::INTEGER                              AS order_month,
        quarter::INTEGER                            AS order_quarter,
        day_of_week::INTEGER                        AS day_of_week,
        is_weekend::BOOLEAN                         AS is_weekend,

        -- ── Metadata ─────────────────────────────────────────────────────────
        etl_loaded_at                               AS etl_loaded_at,
        CURRENT_TIMESTAMP()                         AS dbt_updated_at

    FROM source

    -- Staging-level filters: remove obvious bad records
    WHERE order_id      IS NOT NULL
      AND customer_id   IS NOT NULL
      AND order_date    IS NOT NULL
      AND unit_price    > 0
      AND quantity      > 0

)

SELECT * FROM renamed
