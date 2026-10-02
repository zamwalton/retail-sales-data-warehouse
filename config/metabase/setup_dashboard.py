
"""
setup_dashboard.py
──────────────────────────────────────────────────────────────────────────────
Creates / updates the Metabase Executive Dashboard for the Retail Pipeline.

Snowflake MARTS schema used by this script:

    DIM_CUSTOMERS
        CUSTOMER_KEY
        CUSTOMER_ID
        COUNTRY
        FIRST_ORDER_DATE
        LAST_ORDER_DATE
        TOTAL_ORDERS
        TOTAL_SPEND
        CUSTOMER_SEGMENT
        DBT_UPDATED_AT

    DIM_DATE
        DATE_KEY
        FULL_DATE
        YEAR
        QUARTER
        MONTH
        MONTH_NAME
        WEEK
        DAY_OF_WEEK
        DAY_NAME
        IS_WEEKEND
        IS_HOLIDAY

    DIM_PRODUCTS
        PRODUCT_KEY
        PRODUCT_ID
        CATEGORY
        AVG_PRICE
        DBT_UPDATED_AT

    FCT_SALES
        SALE_KEY
        ORDER_ID
        CUSTOMER_KEY
        PRODUCT_KEY
        DATE_KEY
        STORE_ID
        CHANNEL
        PAYMENT
        STATUS
        QUANTITY
        UNIT_PRICE
        DISCOUNT_PCT
        DISCOUNT_AMOUNT
        GROSS_AMOUNT
        NET_AMOUNT
        DAYS_TO_SHIP
        DBT_UPDATED_AT

Run:
    python config/metabase/setup_dashboard.py

Requirements:
    - Metabase running on http://localhost:3000
    - Metabase admin account already created
    - Snowflake credentials available in .env
    - Snowflake role RETAIL_ANALYST granted to the Snowflake user

Recommended .env variables:

    METABASE_URL=http://localhost:3000
    METABASE_ADMIN_EMAIL=your_email
    METABASE_ADMIN_PASSWORD=your_password

    SNOWFLAKE_ACCOUNT=...
    SNOWFLAKE_USER=...
    SNOWFLAKE_PASSWORD=...
    SNOWFLAKE_DATABASE=...
    SNOWFLAKE_WAREHOUSE=...
    SNOWFLAKE_ROLE=RETAIL_ANALYST
    SNOWFLAKE_SCHEMA=MARTS

IMPORTANT:
    Do not commit .env or real credentials to Git.
"""

import os
import time
from typing import Optional

import requests
from dotenv import load_dotenv
from loguru import logger


# =============================================================================
# ENVIRONMENT
# =============================================================================

load_dotenv()


METABASE_URL = os.getenv(
    "METABASE_URL",
    "http://localhost:3000",
).rstrip("/")

METABASE_ADMIN_EMAIL = os.getenv("METABASE_ADMIN_EMAIL")
METABASE_ADMIN_PASSWORD = os.getenv("METABASE_ADMIN_PASSWORD")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv(
    "SNOWFLAKE_DATABASE",
    "RETAIL_DB",
)
SNOWFLAKE_WAREHOUSE = os.getenv(
    "SNOWFLAKE_WAREHOUSE",
    "RETAIL_WH",
)
SNOWFLAKE_ROLE = os.getenv(
    "SNOWFLAKE_ROLE",
    "RETAIL_ANALYST",
)
SNOWFLAKE_SCHEMA = os.getenv(
    "SNOWFLAKE_SCHEMA",
    "MARTS",
)


# =============================================================================
# CONSTANTS
# =============================================================================

DASHBOARD_NAME = "Retail Executive Dashboard"

DASHBOARD_DESCRIPTION = (
    "Executive retail analytics dashboard covering revenue, customers, "
    "products, countries, channels, and customer churn risk."
)


# =============================================================================
# SNOWFLAKE METABASE CONNECTION
# =============================================================================

SNOWFLAKE_CONFIG = {
    "engine": "snowflake",
    "name": "Retail Snowflake",
    "details": {
        "account": SNOWFLAKE_ACCOUNT,
        "user": SNOWFLAKE_USER,
        "password": SNOWFLAKE_PASSWORD,
        "db": SNOWFLAKE_DATABASE,
        "schema": SNOWFLAKE_SCHEMA,
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "role": SNOWFLAKE_ROLE,
    },
}


# =============================================================================
# DASHBOARD QUESTIONS
# =============================================================================
#
# IMPORTANT:
# These queries use ONLY columns that actually exist in the uploaded
# Snowflake MARTS schema.
#
# Relationships:
#
# FCT_SALES.DATE_KEY
#       -> DIM_DATE.DATE_KEY
#
# FCT_SALES.PRODUCT_KEY
#       -> DIM_PRODUCTS.PRODUCT_KEY
#
# FCT_SALES.CUSTOMER_KEY
#       -> DIM_CUSTOMERS.CUSTOMER_KEY
#
# =============================================================================


QUESTIONS = [

    # -------------------------------------------------------------------------
    # 1. MONTHLY REVENUE TREND
    # -------------------------------------------------------------------------

    {
        "name": "Monthly Revenue Trend",
        "display": "line",

        "sql": """
            SELECT
                d.FULL_DATE AS month,
                ROUND(SUM(f.NET_AMOUNT), 2) AS net_revenue,
                COUNT(DISTINCT f.ORDER_ID) AS total_orders
            FROM MARTS.FCT_SALES f
            INNER JOIN MARTS.DIM_DATE d
                ON f.DATE_KEY = d.DATE_KEY
            GROUP BY d.FULL_DATE
            ORDER BY d.FULL_DATE
        """,

        "row": 0,
        "col": 0,
        "size_x": 12,
        "size_y": 6,
    },


    # -------------------------------------------------------------------------
    # 2. REVENUE BY CATEGORY
    # -------------------------------------------------------------------------

    {
        "name": "Revenue by Category",
        "display": "bar",

        "sql": """
            SELECT
                p.CATEGORY AS product_category,
                ROUND(SUM(f.NET_AMOUNT), 2) AS net_revenue
            FROM MARTS.FCT_SALES f
            INNER JOIN MARTS.DIM_PRODUCTS p
                ON f.PRODUCT_ID= p.PRODUCT_ID
            GROUP BY p.CATEGORY
            ORDER BY net_revenue DESC
        """,

        "row": 6,
        "col": 0,
        "size_x": 8,
        "size_y": 6,
    },


    # -------------------------------------------------------------------------
    # 3. CUSTOMER SEGMENT MIX
    # -------------------------------------------------------------------------

    {
        "name": "Customer Segment Mix",
        "display": "pie",

        "sql": """
            SELECT
                CUSTOMER_SEGMENT,
                COUNT(*) AS customers
            FROM MARTS.DIM_CUSTOMERS
            GROUP BY CUSTOMER_SEGMENT
            ORDER BY customers DESC
        """,

        "row": 6,
        "col": 8,
        "size_x": 4,
        "size_y": 6,
    },


    # -------------------------------------------------------------------------
    # 4. TOP 10 COUNTRIES BY REVENUE
    # -------------------------------------------------------------------------

    {
        "name": "Top 10 Countries by Revenue",
        "display": "bar",

        "sql": """
            SELECT
                c.COUNTRY_CODE,
                ROUND(SUM(f.NET_AMOUNT), 2) AS revenue
            FROM MARTS.FCT_SALES f
            INNER JOIN MARTS.DIM_CUSTOMERS c
                ON f.CUSTOMER_ID = c.CUSTOMER_ID
            GROUP BY c.COUNTRY_CODE
            ORDER BY revenue DESC
            LIMIT 10
        """,

        "row": 12,
        "col": 0,
        "size_x": 6,
        "size_y": 6,
    },


    # -------------------------------------------------------------------------
    # 5. CHANNEL PERFORMANCE
    # -------------------------------------------------------------------------

    {
        "name": "Channel Performance",
        "display": "table",

        "sql": """
            SELECT
                f.SALES_CHANNEL AS channel,
                COUNT(DISTINCT f.ORDER_ID) AS orders,
                ROUND(SUM(f.NET_AMOUNT), 2) AS revenue,
                ROUND(AVG(f.NET_AMOUNT), 2) AS average_order_value,
                ROUND(AVG(f.DAYS_TO_SHIP), 1) AS avg_ship_days
            FROM MARTS.FCT_SALES f
            GROUP BY f.SALES_CHANNEL
            ORDER BY revenue DESC
        """,

        "row": 12,
        "col": 6,
        "size_x": 6,
        "size_y": 6,
    },


    # -------------------------------------------------------------------------
    # 6. CHURN RISK SUMMARY
    # -------------------------------------------------------------------------
    #
    # IMPORTANT:
    # There is NO CHURN_RISK column in DIM_CUSTOMERS.
    #
    # Therefore churn risk is calculated from LAST_ORDER_DATE.
    #
    # Business rule:
    #
    #   > 180 days since last order = High Risk
    #   > 90 days  since last order = Medium Risk
    #   otherwise                  = Low Risk
    #
    # This is a dashboard classification rule, not a physical Snowflake
    # column.
    #
    # -------------------------------------------------------------------------

    {
        "name": "Churn Risk Summary",
        "display": "table",

        "sql": """
            WITH customer_risk AS (
                SELECT
                    CUSTOMER_KEY,
                    CUSTOMER_ID,
                    TOTAL_SPEND,
                    LAST_ORDER_DATE,

                    CASE
                        WHEN LAST_ORDER_DATE IS NULL
                            THEN 'High Risk'

                        WHEN LAST_ORDER_DATE <
                             DATEADD('day', -180, CURRENT_DATE())
                            THEN 'High Risk'

                        WHEN LAST_ORDER_DATE <
                             DATEADD('day', -90, CURRENT_DATE())
                            THEN 'Medium Risk'

                        ELSE 'Low Risk'
                    END AS CHURN_RISK

                FROM MARTS.DIM_CUSTOMERS
            )

            SELECT
                CHURN_RISK,
                COUNT(*) AS customers,
                ROUND(SUM(TOTAL_SPEND), 2) AS customer_revenue
            FROM customer_risk
            GROUP BY CHURN_RISK
            ORDER BY
                CASE CHURN_RISK
                    WHEN 'High Risk' THEN 1
                    WHEN 'Medium Risk' THEN 2
                    WHEN 'Low Risk' THEN 3
                    ELSE 4
                END
        """,

        "row": 18,
        "col": 0,
        "size_x": 12,
        "size_y": 4,
    },
]


# =============================================================================
# METABASE CLIENT
# =============================================================================


class MetabaseClient:

    def __init__(self, base_url: str):
        self.base = base_url.rstrip("/")

        self.session = requests.Session()

        self.session.headers.update(
            {
                "Content-Type": "application/json",
            }
        )


    # -------------------------------------------------------------------------
    # GENERIC GET
    # -------------------------------------------------------------------------

    def get(self, endpoint: str):
        response = self.session.get(
            f"{self.base}{endpoint}",
            timeout=60,
        )

        response.raise_for_status()

        return response.json()


    # -------------------------------------------------------------------------
    # AUTHENTICATION
    # -------------------------------------------------------------------------

    def authenticate(
        self,
        email: str,
        password: str,
    ) -> None:

        if not email:
            raise ValueError(
                "METABASE_ADMIN_EMAIL is missing from .env"
            )

        if not password:
            raise ValueError(
                "METABASE_ADMIN_PASSWORD is missing from .env"
            )

        response = self.session.post(
            f"{self.base}/api/session",
            json={
                "username": email,
                "password": password,
            },
            timeout=60,
        )

        if response.status_code != 200:
            logger.error(
                f"Metabase authentication failed: "
                f"{response.status_code}"
            )
            logger.error(response.text)

        response.raise_for_status()

        session_id = response.json()["id"]

        self.session.headers["X-Metabase-Session"] = session_id

        logger.success(
            "Authenticated with Metabase"
        )


    # -------------------------------------------------------------------------
    # FIND DATABASE
    # -------------------------------------------------------------------------

    def find_database(
        self,
        name: str,
    ) -> Optional[int]:

        response = self.get("/api/database")

        if isinstance(response, dict):
            databases = response.get("data", [])
        elif isinstance(response, list):
            databases = response
        else:
            databases = []

        for database in databases:

            if database.get("name") == name:

                database_id = database.get("id")

                logger.info(
                    f"Existing database found: "
                    f"'{name}' (id={database_id})"
                )

                return database_id

        return None


    # -------------------------------------------------------------------------
    # CREATE DATABASE
    # -------------------------------------------------------------------------

    def create_database(
        self,
        config: dict,
    ) -> int:

        existing_id = self.find_database(
            config["name"]
        )

        if existing_id is not None:

            logger.info(
                f"Reusing existing database: "
                f"id={existing_id}"
            )

            return existing_id

        response = self.session.post(
            f"{self.base}/api/database",
            json=config,
            timeout=120,
        )

        if response.status_code >= 400:

            logger.error(
                f"Database creation failed: "
                f"{response.status_code}"
            )

            logger.error(response.text)

        response.raise_for_status()

        database_id = response.json()["id"]

        logger.success(
            f"Database created: "
            f"'{config['name']}' "
            f"(id={database_id})"
        )

        return database_id


    # -------------------------------------------------------------------------
    # FIND DASHBOARD
    # -------------------------------------------------------------------------

    def find_dashboard(
        self,
        name: str,
    ) -> Optional[int]:

        response = self.get("/api/dashboard")

        if isinstance(response, dict):
            dashboards = response.get("data", [])
        elif isinstance(response, list):
            dashboards = response
        else:
            dashboards = []

        for dashboard in dashboards:

            if dashboard.get("name") == name:

                dashboard_id = dashboard.get("id")

                logger.info(
                    f"Existing dashboard found: "
                    f"'{name}' "
                    f"(id={dashboard_id})"
                )

                return dashboard_id

        return None


    # -------------------------------------------------------------------------
    # CREATE DASHBOARD
    # -------------------------------------------------------------------------

    def create_dashboard(
        self,
        name: str,
        description: str,
    ) -> int:

        existing_id = self.find_dashboard(name)

        if existing_id is not None:

            logger.info(
                f"Reusing existing dashboard: "
                f"id={existing_id}"
            )

            return existing_id

        payload = {
            "name": name,
            "description": description,
        }

        response = self.session.post(
            f"{self.base}/api/dashboard",
            json=payload,
            timeout=60,
        )

        if response.status_code >= 400:

            logger.error(
                f"Dashboard creation failed: "
                f"{response.status_code}"
            )

            logger.error(response.text)

        response.raise_for_status()

        dashboard_id = response.json()["id"]

        logger.success(
            f"Dashboard created: "
            f"'{name}' "
            f"(id={dashboard_id})"
        )

        return dashboard_id


    # -------------------------------------------------------------------------
    # FIND CARD
    # -------------------------------------------------------------------------

    def find_card(
        self,
        name: str,
    ) -> Optional[int]:

        response = self.get("/api/card")

        if isinstance(response, list):

            cards = response

        elif isinstance(response, dict):

            cards = response.get("data", [])

        else:

            cards = []

        for card in cards:

            if card.get("name") == name:

                card_id = card.get("id")

                logger.info(
                    f"Existing question found: "
                    f"'{name}' "
                    f"(id={card_id})"
                )

                return card_id

        return None


    # -------------------------------------------------------------------------
    # CREATE OR UPDATE QUESTION
    # -------------------------------------------------------------------------

    def create_question(
        self,
        name: str,
        database_id: int,
        sql: str,
        display: str = "table",
    ) -> int:

        existing_id = self.find_card(name)

        payload = {
            "name": name,
            "display": display,

            "dataset_query": {
                "type": "native",

                "native": {
                    "query": sql,
                },

                "database": database_id,
            },

            "visualization_settings": {},
        }


        # ---------------------------------------------------------------------
        # UPDATE EXISTING QUESTION
        # ---------------------------------------------------------------------

        if existing_id is not None:

            response = self.session.put(
                f"{self.base}/api/card/{existing_id}",
                json=payload,
                timeout=60,
            )

            if response.status_code >= 400:

                logger.error(
                    f"Question update failed for "
                    f"'{name}': "
                    f"{response.status_code}"
                )

                logger.error(response.text)

            response.raise_for_status()

            logger.success(
                f"Question updated: "
                f"'{name}' "
                f"(id={existing_id})"
            )

            return existing_id


        # ---------------------------------------------------------------------
        # CREATE NEW QUESTION
        # ---------------------------------------------------------------------

        response = self.session.post(
            f"{self.base}/api/card",
            json=payload,
            timeout=60,
        )

        if response.status_code >= 400:

            logger.error(
                f"Question creation failed for "
                f"'{name}': "
                f"{response.status_code}"
            )

            logger.error(response.text)

        response.raise_for_status()

        card_id = response.json()["id"]

        logger.success(
            f"Question created: "
            f"'{name}' "
            f"(id={card_id})"
        )

        return card_id


    # -------------------------------------------------------------------------
    # GET DASHBOARD
    # -------------------------------------------------------------------------

    def get_dashboard(
        self,
        dashboard_id: int,
    ) -> dict:

        return self.get(
            f"/api/dashboard/{dashboard_id}"
        )


    # -------------------------------------------------------------------------
    # UPDATE DASHBOARD CARDS
    # -------------------------------------------------------------------------

    def update_dashboard_cards(
        self,
        dashboard_id: int,
        question_cards: list,
    ) -> None:

        dashboard = self.get_dashboard(
            dashboard_id
        )

        existing_dashcards = dashboard.get(
            "dashcards",
            [],
        )

        existing_dashcards_by_card_id = {}

        for dashcard in existing_dashcards:

            card_id = dashcard.get("card_id")

            if card_id is not None:
                existing_dashcards_by_card_id[
                    card_id
                ] = dashcard


        final_dashcards = []

        # Temporary IDs for newly-created dashboard cards.
        #
        # Metabase requires every dashcard ID in the submitted payload
        # to be unique. Existing cards keep their real IDs.
        #
        next_temp_id = -1


        for question in question_cards:

            card_id = question["card_id"]

            row = question["row"]
            col = question["col"]
            size_x = question["size_x"]
            size_y = question["size_y"]


            # ---------------------------------------------------------------
            # EXISTING DASHCARD
            # ---------------------------------------------------------------

            if card_id in existing_dashcards_by_card_id:

                dashcard = dict(
                    existing_dashcards_by_card_id[
                        card_id
                    ]
                )

                dashcard["row"] = row
                dashcard["col"] = col
                dashcard["size_x"] = size_x
                dashcard["size_y"] = size_y

                final_dashcards.append(
                    dashcard
                )

            # ---------------------------------------------------------------
            # NEW DASHCARD
            # ---------------------------------------------------------------

            else:

                dashcard = {
                    "id": next_temp_id,
                    "card_id": card_id,
                    "row": row,
                    "col": col,
                    "size_x": size_x,
                    "size_y": size_y,
                    "series": [],
                    "parameter_mappings": [],
                }

                final_dashcards.append(
                    dashcard
                )

                next_temp_id -= 1


        payload = {
            "dashcards": final_dashcards,
            "tabs": dashboard.get(
                "tabs",
                [],
            ),
        }


        response = self.session.put(
            f"{self.base}/api/dashboard/{dashboard_id}",
            json=payload,
            timeout=60,
        )


        if response.status_code >= 400:

            logger.error(
                f"Dashboard update failed: "
                f"{response.status_code}"
            )

            logger.error(response.text)

        response.raise_for_status()

        logger.success(
            f"Dashboard updated successfully: "
            f"id={dashboard_id}"
        )


# =============================================================================
# METABASE HEALTH CHECK
# =============================================================================


def wait_for_metabase(
    base_url: str,
    timeout: int = 60,
) -> None:

    logger.info(
        "Waiting for Metabase to be ready..."
    )

    deadline = time.time() + timeout


    while time.time() < deadline:

        try:

            response = requests.get(
                f"{base_url}/api/health",
                timeout=5,
            )

            if response.status_code == 200:

                logger.success(
                    "Metabase is ready"
                )

                return

        except requests.RequestException:
            pass

        time.sleep(3)


    raise TimeoutError(
        f"Metabase not ready after "
        f"{timeout} seconds"
    )


# =============================================================================
# CONFIGURATION VALIDATION
# =============================================================================


def validate_configuration() -> None:

    required = {
        "METABASE_ADMIN_EMAIL": METABASE_ADMIN_EMAIL,
        "METABASE_ADMIN_PASSWORD": METABASE_ADMIN_PASSWORD,

        "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
        "SNOWFLAKE_USER": SNOWFLAKE_USER,
        "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
    }


    missing = [
        key
        for key, value in required.items()
        if not value
    ]


    if missing:

        raise ValueError(
            "Missing required environment variables: "
            + ", ".join(missing)
        )


# =============================================================================
# MAIN
# =============================================================================


def main() -> None:

    logger.info(
        "Starting Metabase dashboard setup..."
    )


    # -------------------------------------------------------------------------
    # Validate environment
    # -------------------------------------------------------------------------

    validate_configuration()


    # -------------------------------------------------------------------------
    # Wait for Metabase
    # -------------------------------------------------------------------------

    wait_for_metabase(
        METABASE_URL
    )


    # -------------------------------------------------------------------------
    # Authenticate
    # -------------------------------------------------------------------------

    client = MetabaseClient(
        METABASE_URL
    )

    client.authenticate(
        METABASE_ADMIN_EMAIL,
        METABASE_ADMIN_PASSWORD,
    )


    # -------------------------------------------------------------------------
    # Create / reuse Snowflake connection
    # -------------------------------------------------------------------------

    database_id = client.create_database(
        SNOWFLAKE_CONFIG
    )


    # -------------------------------------------------------------------------
    # Create / reuse dashboard
    # -------------------------------------------------------------------------

    dashboard_id = client.create_dashboard(
        DASHBOARD_NAME,
        DASHBOARD_DESCRIPTION,
    )


    # -------------------------------------------------------------------------
    # Create / update six questions
    # -------------------------------------------------------------------------

    dashboard_cards = []


    for question in QUESTIONS:

        logger.info(
            f"Processing question: "
            f"{question['name']}"
        )


        card_id = client.create_question(
            name=question["name"],
            database_id=database_id,
            sql=question["sql"],
            display=question["display"],
        )


        dashboard_cards.append(
            {
                "card_id": card_id,

                "row": question["row"],
                "col": question["col"],

                "size_x": question["size_x"],
                "size_y": question["size_y"],
            }
        )


        time.sleep(0.5)


    # -------------------------------------------------------------------------
    # Assemble dashboard
    # -------------------------------------------------------------------------

    client.update_dashboard_cards(
        dashboard_id=dashboard_id,
        question_cards=dashboard_cards,
    )


    # -------------------------------------------------------------------------
    # Final output
    # -------------------------------------------------------------------------

    logger.success(
        ""
    )

    logger.success(
        "============================================================"
    )

    logger.success(
        "METABASE DASHBOARD SETUP COMPLETE"
    )

    logger.success(
        "============================================================"
    )

    logger.success(
        f"Snowflake database ID : {database_id}"
    )

    logger.success(
        f"Dashboard ID          : {dashboard_id}"
    )

    logger.success(
        f"Dashboard URL         : "
        f"{METABASE_URL}/dashboard/{dashboard_id}"
    )

    logger.success(
        "Questions configured  : 6"
    )

    logger.success(
        "============================================================"
    )


# =============================================================================
# ENTRY POINT
# =============================================================================


if __name__ == "__main__":
    main()
