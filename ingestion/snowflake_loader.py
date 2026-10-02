"""
snowflake_loader.py
────────────────────
Utility to load Parquet from S3 into Snowflake via COPY INTO.
Called by Airflow or standalone after Spark ETL completes.

Run: python ingestion/snowflake_loader.py
"""

import os
from datetime import datetime

import snowflake.connector
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


def get_connection():
    return snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "RETAIL_WH"),
        database  = os.getenv("SNOWFLAKE_DATABASE",  "RETAIL_DB"),
        schema    = os.getenv("SNOWFLAKE_SCHEMA",    "RAW"),
    )


def run_copy_into(conn) -> dict:
    """Runs COPY INTO and returns load statistics."""
    cur = conn.cursor()

    logger.info("Running COPY INTO orders from S3 staging stage...")
    cur.execute("""
        COPY INTO orders (
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
        PURGE    = FALSE
    """)

    results = cur.fetchall()
    total_loaded = sum(r[3] for r in results)   # rows_loaded column
    total_errors = sum(r[4] for r in results)   # rows_errors column

    logger.success(f"COPY INTO complete — loaded: {total_loaded:,}, errors: {total_errors:,}")
    return {"rows_loaded": total_loaded, "rows_errors": total_errors}


def verify_load(conn) -> dict:
    """Runs a quick sanity check query on the loaded table."""
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*)                       AS total_rows,
            COUNT(DISTINCT customer_id)    AS unique_customers,
            MIN(order_date)                AS earliest_order,
            MAX(order_date)                AS latest_order,
            ROUND(SUM(net_amount) / 1e6, 2) AS revenue_millions
        FROM orders
    """)
    row = cur.fetchone()
    stats = {
        "total_rows":       row[0],
        "unique_customers": row[1],
        "earliest_order":   str(row[2]),
        "latest_order":     str(row[3]),
        "revenue_millions": row[4],
    }
    logger.info("Load verification:")
    for k, v in stats.items():
        logger.info(f"  {k}: {v}")
    return stats


def main() -> None:
    conn = get_connection()
    try:
        run_copy_into(conn)
        verify_load(conn)
    finally:
        conn.close()
        logger.info("Snowflake connection closed")


if __name__ == "__main__":
    main()
