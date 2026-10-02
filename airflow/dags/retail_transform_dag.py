"""
retail_transform_dag.py
──────────────────────────────────────────────────────────────────────────────
DAG 2: dbt transformation pipeline.

Triggered by retail_ingestion_dag after the quality gate passes.

Runs dbt models in dependency order:

    1. dbt deps
    2. dbt source freshness
    3. Build staging models
    4. Test staging models
    5. Build dimension models
    6. Build fact model
    7. Test marts
    8. Generate dbt documentation
    9. Notify pipeline success

Schedule:
    Triggered only — no cron.
    Downstream of retail_ingestion_dag.

Owner:
    data-engineering

SLA:
    30 minutes
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# =============================================================================
# Configuration
# =============================================================================

DBT_DIR = "/opt/dbt"

DBT_PROFILES = "/opt/dbt/profiles.yml"

DBT_CMD_BASE = f"cd {DBT_DIR} && dbt"


# =============================================================================
# Default Arguments
# =============================================================================

default_args = {
    "owner": "data-engineering",

    "depends_on_past": False,

    "start_date": datetime(2024, 1, 1),

    "email": [
        "data-alerts@retail.com"
    ],

    "email_on_failure": True,

    "email_on_retry": False,

    "retries": 1,

    "retry_delay": timedelta(
        minutes=3
    ),

    "execution_timeout": timedelta(
        minutes=30
    ),
}


# =============================================================================
# Task Functions
# =============================================================================

def notify_success(**context) -> None:
    """
    Log a successful dbt pipeline execution.

    This is currently a notification stub.

    In production, this can be replaced with:
        - Slack webhook
        - Microsoft Teams
        - Email
        - PagerDuty
        - Airflow notification service
    """

    ti = context["task_instance"]

    dag = context["dag"].dag_id

    execution_date = context["ds"]

    print(
        "=" * 80
    )

    print(
        "DBT TRANSFORMATION PIPELINE SUCCESS"
    )

    print(
        f"DAG           : {dag}"
    )

    print(
        f"Execution date: {execution_date}"
    )

    print(
        "Status        : SUCCESS"
    )

    print(
        "=" * 80
    )

    # In production:
    #
    # requests.post(
    #     SLACK_WEBHOOK,
    #     json={
    #         "text": (
    #             f"Retail dbt pipeline completed successfully "
    #             f"for {execution_date}"
    #         )
    #     }
    # )


def generate_dbt_docs(**context) -> None:
    """
    Generate dbt documentation artifacts.
    """

    import subprocess

    command = (
        f"cd {DBT_DIR} && "
        f"dbt docs generate "
        f"--profiles-dir {DBT_PROFILES}"
    )

    print(
        f"Running: {command}"
    )

    subprocess.run(
        command,
        shell=True,
        check=True,
    )

    print(
        "dbt documentation generated successfully."
    )

    print(
        "Output: target/catalog.json"
    )


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(

    dag_id="retail_transform_dag",

    default_args=default_args,

    # Triggered by retail_ingestion_dag
    schedule_interval=None,

    catchup=False,

    max_active_runs=1,

    tags=[
        "retail",
        "dbt",
        "transform",
        "snowflake",
        "day2",
    ],

    doc_md="""
    # Retail Transform DAG

    Executes the dbt transformation pipeline after the ingestion DAG
    successfully completes.

    ## Pipeline

    dbt deps
    ↓
    dbt source freshness
    ↓
    staging models
    ↓
    staging tests
    ↓
    dimension models
    ↓
    fact model
    ↓
    marts tests
    ↓
    dbt documentation
    ↓
    success notification

    ## Target

    Snowflake:

    `RETAIL_DB`

    ## Schemas

    Staging:

    `RETAIL_DB.STAGING`

    Marts:

    `RETAIL_DB.MARTS`

    ## Trigger

    This DAG is triggered by:

    `retail_ingestion_dag`
    """,

) as dag:

    # =========================================================================
    # Task 1: Install dbt packages
    # =========================================================================

    dbt_deps = BashOperator(

        task_id="dbt_install_packages",

        bash_command=(
            f"{DBT_CMD_BASE} deps "
            f"--profiles-dir {DBT_PROFILES}"
        ),
    )


    # =========================================================================
    # Task 2: Check dbt Source Freshness
    # =========================================================================

    dbt_freshness = BashOperator(

        task_id="dbt_source_freshness",

        bash_command=(
            f"{DBT_CMD_BASE} source freshness "
            f"--profiles-dir {DBT_PROFILES}"
        ),
    )


    # =========================================================================
    # Task 3: Build Staging Layer
    # =========================================================================

    dbt_staging = BashOperator(

        task_id="dbt_build_staging",

        bash_command=(
            f"{DBT_CMD_BASE} run "
            "--select staging "
            f"--profiles-dir {DBT_PROFILES}"
        ),

        sla=timedelta(
            minutes=5
        ),
    )


    # =========================================================================
    # Task 4: Test Staging Layer
    # =========================================================================

    dbt_test_staging = BashOperator(

        task_id="dbt_test_staging",

        bash_command=(
            f"{DBT_CMD_BASE} test "
            "--select staging "
            f"--profiles-dir {DBT_PROFILES}"
        ),
    )


    # =========================================================================
    # Task 5: Build Dimension Tables
    # =========================================================================

    dbt_dims = BashOperator(

        task_id="dbt_build_dimensions",

        bash_command=(
            f"{DBT_CMD_BASE} run "
            "--select "
            "dim_customers "
            "dim_products "
            "dim_date "
            f"--profiles-dir {DBT_PROFILES}"
        ),

        sla=timedelta(
            minutes=10
        ),
    )


    # =========================================================================
    # Task 6: Build Fact Table
    # =========================================================================

    dbt_fact = BashOperator(

        task_id="dbt_build_fact_sales",

        bash_command=(
            f"{DBT_CMD_BASE} run "
            "--select fct_sales "
            f"--profiles-dir {DBT_PROFILES}"
        ),

        sla=timedelta(
            minutes=10
        ),
    )


    # =========================================================================
    # Task 7: Test Marts
    # =========================================================================

    dbt_test_marts = BashOperator(

        task_id="dbt_test_marts",

        bash_command=(
            f"{DBT_CMD_BASE} test "
            "--select marts "
            f"--profiles-dir {DBT_PROFILES}"
        ),
    )


    # =========================================================================
    # Task 8: Generate dbt Documentation
    # =========================================================================

    generate_docs = PythonOperator(

        task_id="generate_dbt_docs",

        python_callable=generate_dbt_docs,
    )


    # =========================================================================
    # Task 9: Success Notification
    # =========================================================================

    success_notify = PythonOperator(

        task_id="notify_pipeline_success",

        python_callable=notify_success,
    )


    # =========================================================================
    # Task Dependencies
    # =========================================================================

    (
        dbt_deps
        >> dbt_freshness
        >> dbt_staging
        >> dbt_test_staging
        >> dbt_dims
        >> dbt_fact
        >> dbt_test_marts
        >> generate_docs
        >> success_notify
    )