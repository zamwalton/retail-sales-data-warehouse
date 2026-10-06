"""
load_snowflake_raw.py

Loads the latest Spark-generated Parquet data from S3 staging
into Snowflake RAW.ORDERS.

Flow:

    S3 staging/orders/
        ↓
    Snowflake temporary load table
        ↓
    Replace RAW.ORDERS snapshot
"""

import os

import snowflake.connector

# =============================================================================
# Configuration
# =============================================================================

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "RETAIL_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "RETAIL_DB")
SNOWFLAKE_SCHEMA = "RAW"
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "RETAIL_ENGINEER")

SNOWFLAKE_STAGE = os.getenv(
    "SNOWFLAKE_STAGE",
    "RETAIL_DB.RAW.ORDERS_STAGE",
)

TARGET_TABLE = "RETAIL_DB.RAW.ORDERS"
TEMP_TABLE = "RETAIL_DB.RAW.ORDERS_LOAD_TEMP"


# =============================================================================
# Validation
# =============================================================================


def validate_configuration() -> None:

    required = {
        "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
        "SNOWFLAKE_USER": SNOWFLAKE_USER,
        "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
    }

    missing = [name for name, value in required.items() if not value]

    if missing:
        raise RuntimeError(
            "Missing Snowflake environment variables: " + ", ".join(missing)
        )


# =============================================================================
# Main
# =============================================================================


def main() -> None:

    validate_configuration()

    print("=" * 80)
    print("SNOWFLAKE RAW LOAD")
    print("=" * 80)

    connection = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    cursor = connection.cursor()

    try:

        # ---------------------------------------------------------------------
        # 1. Create temporary load table
        # ---------------------------------------------------------------------

        print("Creating temporary load table...")

        cursor.execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE {TEMP_TABLE}
            LIKE {TARGET_TABLE}
            """)

        # ---------------------------------------------------------------------
        # 2. Load S3 staging data into temporary table
        # ---------------------------------------------------------------------

        print(f"Loading S3 stage into {TEMP_TABLE}...")

        copy_sql = f"""
        COPY INTO {TEMP_TABLE}
        (
            order_id,
            customer_id,
            product_id,
            store_id,
            category,
            quantity,
            unit_price,
            discount_pct,
            order_date,
            ship_date,
            status,
            country,
            channel,
            payment,
            gross_amount,
            net_amount,
            discount_amount,
            year,
            month,
            quarter,
            day_of_week,
            is_weekend,
            days_to_ship
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
            FROM @{SNOWFLAKE_STAGE}
        )
        PATTERN = '.*[.]parquet'
        ON_ERROR = 'ABORT_STATEMENT'
        """

        cursor.execute(copy_sql)

        copy_results = cursor.fetchall()

        print("COPY INTO result:")

        for row in copy_results:
            print(row)

        # ---------------------------------------------------------------------
        # 3. Validate temporary load
        # ---------------------------------------------------------------------

        cursor.execute(f"""
            SELECT
                COUNT(*) AS row_count,
                MAX(etl_loaded_at) AS latest_load
            FROM {TEMP_TABLE}
            """)

        row_count, latest_load = cursor.fetchone()

        print()
        print(f"Temporary row count : {row_count:,}")
        print(f"Temporary latest load: {latest_load}")

        if row_count == 0:
            raise RuntimeError("Snowflake RAW load produced zero rows.")

        # ---------------------------------------------------------------------
        # 4. Replace RAW snapshot atomically
        # ---------------------------------------------------------------------

        print()
        print("Replacing RAW.ORDERS snapshot...")

        cursor.execute("BEGIN")

        try:

            cursor.execute(f"""
                TRUNCATE TABLE {TARGET_TABLE}
                """)

            cursor.execute(f"""
                INSERT INTO {TARGET_TABLE}
                (
                    order_id,
                    customer_id,
                    product_id,
                    store_id,
                    category,
                    quantity,
                    unit_price,
                    discount_pct,
                    order_date,
                    ship_date,
                    status,
                    country,
                    channel,
                    payment,
                    gross_amount,
                    net_amount,
                    discount_amount,
                    year,
                    month,
                    quarter,
                    day_of_week,
                    is_weekend,
                    days_to_ship
                )
                SELECT
                    order_id,
                    customer_id,
                    product_id,
                    store_id,
                    category,
                    quantity,
                    unit_price,
                    discount_pct,
                    order_date,
                    ship_date,
                    status,
                    country,
                    channel,
                    payment,
                    gross_amount,
                    net_amount,
                    discount_amount,
                    year,
                    month,
                    quarter,
                    day_of_week,
                    is_weekend,
                    days_to_ship
                FROM {TEMP_TABLE}
                """)

            cursor.execute("COMMIT")

        except Exception:
            cursor.execute("ROLLBACK")
            raise

        # ---------------------------------------------------------------------
        # 5. Final verification
        # ---------------------------------------------------------------------

        cursor.execute(f"""
            SELECT
                COUNT(*) AS total_orders,
                COUNT(DISTINCT order_id) AS unique_orders,
                MAX(etl_loaded_at) AS latest_load
            FROM {TARGET_TABLE}
            """)

        total_orders, unique_orders, latest_load = cursor.fetchone()

        print()
        print("=" * 80)
        print("SNOWFLAKE RAW LOAD COMPLETE")
        print("=" * 80)
        print(f"Total orders  : {total_orders:,}")
        print(f"Unique orders : {unique_orders:,}")
        print(f"Latest load   : {latest_load}")
        print("=" * 80)

        if total_orders != unique_orders:
            raise RuntimeError("RAW.ORDERS contains duplicate order_id values.")

    finally:

        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()
