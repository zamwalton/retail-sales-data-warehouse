-- dim_date.sql
-- Calendar date dimension: covers Jan 2023 – Dec 2025.
-- Pre-generates 1,096 rows using Snowflake's GENERATOR function.
-- Grain: one row per calendar date.

{{
  config(
    materialized = 'table',
    tags         = ['marts', 'dimension', 'date']
  )
}}

WITH date_spine AS (

    SELECT
        DATEADD('day', ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1,
                '{{ var("start_date") }}'::DATE)   AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 1096))  -- 3 years

),

enriched AS (

    SELECT
        TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD'))   AS date_key,
        full_date,

        -- Year / Quarter / Month
        YEAR(full_date)                             AS year,
        QUARTER(full_date)                          AS quarter,
        'Q' || QUARTER(full_date)                   AS quarter_label,
        MONTH(full_date)                            AS month,
        TO_CHAR(full_date, 'MMMM')                  AS month_name,
        TO_CHAR(full_date, 'Mon')                   AS month_short,

        -- Week
        WEEKOFYEAR(full_date)                       AS week_of_year,
        YEAROFWEEK(full_date)                       AS year_of_week,

        -- Day
        DAYOFYEAR(full_date)                        AS day_of_year,
        DAY(full_date)                              AS day_of_month,
        DAYOFWEEK(full_date)                        AS day_of_week,    -- 0=Sun
        TO_CHAR(full_date, 'DDDD')                  AS day_name,
        TO_CHAR(full_date, 'DDD')                   AS day_short,

        -- Flags
        CASE WHEN DAYOFWEEK(full_date) IN (0, 6)
             THEN TRUE ELSE FALSE END               AS is_weekend,

        -- Period comparisons
        CASE WHEN full_date <= CURRENT_DATE()
             THEN TRUE ELSE FALSE END               AS is_past,
        CASE WHEN YEAR(full_date) = YEAR(CURRENT_DATE())
              AND MONTH(full_date) = MONTH(CURRENT_DATE())
             THEN TRUE ELSE FALSE END               AS is_current_month,
        CASE WHEN YEAR(full_date) = YEAR(CURRENT_DATE())
             THEN TRUE ELSE FALSE END               AS is_current_year

    FROM date_spine

)

SELECT * FROM enriched
ORDER BY date_key
