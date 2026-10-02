{% snapshot customers_snapshot %}

  {#
    SCD Type 2 snapshot: tracks historical changes to customer segments.

    When a customer moves from 'silver' → 'gold', dbt:
      - Closes the old record (dbt_valid_to = NOW())
      - Inserts a new record (dbt_valid_from = NOW(), dbt_valid_to = NULL)

    Run: dbt snapshot
  #}

  {{
    config(
      target_schema  = 'snapshots',
      unique_key     = 'customer_id',
      strategy       = 'check',
      check_cols     = ['customer_segment', 'churn_risk', 'total_orders', 'total_spend'],
      invalidate_hard_deletes = True
    )
  }}

  SELECT
    customer_key,
    customer_id,
    country_code,
    first_order_date,
    last_order_date,
    total_orders,
    total_spend,
    avg_order_value,
    customer_segment,
    churn_risk
  FROM {{ ref('dim_customers') }}

{% endsnapshot %}