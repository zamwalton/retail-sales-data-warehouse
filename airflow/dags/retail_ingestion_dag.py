"""
retail_ingestion_dag.py
──────────────────────────────────────────────────────────────────────────────
DAG 1: Raw data ingestion pipeline.

Runs daily at 01:00 UTC.
Triggers the transform DAG after successful ingestion and data-quality checks.

Pipeline:

    1. Check AWS S3 source freshness
    2. Generate synthetic source data
    3. Upload raw data to AWS S3
    4. Run Spark ETL
    5. Run data-quality gate
    6. Log S3 metrics
    7. Trigger transform DAG

Schedule:
    0 1 * * *  (01:00 UTC daily)

Owner:
    data-engineering

S3:
    Bucket: s3://retail-pipeline-zam-2026/

Raw:
    s3://retail-pipeline-zam-2026/raw/orders/

Staging:
    s3://retail-pipeline-zam-2026/staging/orders/
"""

from datetime import datetime, timedelta, timezone
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule


# =============================================================================
# Configuration
# =============================================================================

S3_BUCKET = os.getenv(
    "S3_BUCKET",
    "retail-pipeline-zam-2026",
)

AWS_REGION = os.getenv(
    "AWS_DEFAULT_REGION",
    "us-east-1",
)

RAW_PREFIX = "raw/orders/"
STAGING_PREFIX = "staging/orders/"


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

    "retries": 2,

    "retry_delay": timedelta(
        minutes=5
    ),

    "execution_timeout": timedelta(
        minutes=90
    ),
}


# =============================================================================
# Helper: Create AWS S3 Client
# =============================================================================

def get_s3_client():
    """
    Create an AWS S3 client.

    Credentials are intentionally NOT hardcoded here.

    boto3 automatically reads:

        AWS_ACCESS_KEY_ID
        AWS_SECRET_ACCESS_KEY
        AWS_DEFAULT_REGION

    from the Airflow container environment.
    """

    import boto3

    return boto3.client(
        "s3",
        region_name=AWS_REGION,
    )


# =============================================================================
# Task 1: Check AWS S3 Source Freshness
# =============================================================================

def check_source_freshness(**context) -> None:
    """
    Check that raw source data exists in AWS S3 and is not older
    than 25 hours.

    Expected location:

        s3://retail-pipeline-zam-2026/raw/orders/
    """

    s3 = get_s3_client()

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    latest_object = None

    for page in paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix=RAW_PREFIX,
    ):

        for obj in page.get(
            "Contents",
            [],
        ):

            # Ignore S3 folder marker objects
            if obj["Key"].endswith("/"):
                continue

            if (
                latest_object is None
                or obj["LastModified"]
                > latest_object["LastModified"]
            ):
                latest_object = obj

    # -------------------------------------------------------------------------
    # No files found
    # -------------------------------------------------------------------------

    if latest_object is None:

        raise FileNotFoundError(
            f"No raw files found in "
            f"s3://{S3_BUCKET}/{RAW_PREFIX}"
        )

    # -------------------------------------------------------------------------
    # Calculate file age
    # -------------------------------------------------------------------------

    now = datetime.now(
        timezone.utc
    )

    age_hours = (
        now
        - latest_object["LastModified"]
    ).total_seconds() / 3600

    print(
        "=" * 80
    )

    print(
        "AWS S3 SOURCE FRESHNESS CHECK"
    )

    print(
        f"Bucket       : {S3_BUCKET}"
    )

    print(
        f"Prefix       : {RAW_PREFIX}"
    )

    print(
        f"Latest file  : {latest_object['Key']}"
    )

    print(
        f"Last modified: {latest_object['LastModified']}"
    )

    print(
        f"Age          : {age_hours:.2f} hours"
    )

    print(
        "=" * 80
    )

    # -------------------------------------------------------------------------
    # Freshness threshold
    # -------------------------------------------------------------------------

    if age_hours > 25:

        raise ValueError(
            f"Source data is stale: "
            f"{age_hours:.1f} hours old. "
            f"Expected less than 25 hours."
        )

    print(
        "Source freshness check PASSED."
    )


# =============================================================================
# Task 2: Count S3 Objects
# =============================================================================

def count_s3_objects(
    s3,
    prefix: str,
) -> int:
    """
    Count objects under a specific S3 prefix.

    Uses pagination so the function continues to work when
    the number of objects exceeds the default S3 response size.
    """

    count = 0

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    for page in paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix=prefix,
    ):

        for obj in page.get(
            "Contents",
            [],
        ):

            if not obj["Key"].endswith("/"):
                count += 1

    return count


# =============================================================================
# Task 3: Log S3 Metrics
# =============================================================================

def log_s3_metrics(**context) -> dict:
    """
    Log raw and staging S3 object counts.

    Metrics are pushed to XCom for downstream monitoring.
    """

    s3 = get_s3_client()

    raw_count = count_s3_objects(
        s3,
        RAW_PREFIX,
    )

    staging_count = count_s3_objects(
        s3,
        STAGING_PREFIX,
    )

    print(
        "=" * 80
    )

    print(
        "AWS S3 PIPELINE METRICS"
    )

    print(
        f"Bucket           : {S3_BUCKET}"
    )

    print(
        f"Raw objects      : {raw_count:,}"
    )

    print(
        f"Staging objects  : {staging_count:,}"
    )

    print(
        "=" * 80
    )

    metrics = {
        "bucket": S3_BUCKET,
        "raw_objects": raw_count,
        "staging_objects": staging_count,
    }

    context[
        "task_instance"
    ].xcom_push(
        key="s3_metrics",
        value=metrics,
    )

    return metrics


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(

    dag_id="retail_ingestion_dag",

    default_args=default_args,

    schedule_interval="0 1 * * *",

    catchup=False,

    max_active_runs=1,

    tags=[
        "retail",
        "ingestion",
        "aws",
        "s3",
        "spark",
    ],

    doc_md="""
    # Retail Ingestion DAG

    Production-style ingestion pipeline for the retail analytics platform.

    ## Pipeline

    AWS S3 raw source
    ↓
    Synthetic data generation
    ↓
    S3 upload
    ↓
    Spark transformation
    ↓
    Data quality validation
    ↓
    S3 metrics
    ↓
    Transform DAG

    ## AWS S3

    Bucket:

    `s3://retail-pipeline-zam-2026/`

    Raw data:

    `s3://retail-pipeline-zam-2026/raw/orders/`

    Spark staging:

    `s3://retail-pipeline-zam-2026/staging/orders/`

    ## Schedule

    Daily at 01:00 UTC.
    """,

) as dag:

    # =========================================================================
    # Task 1: Check S3 Source Freshness
    # =========================================================================

    check_freshness = PythonOperator(

        task_id="check_source_freshness",

        python_callable=check_source_freshness,
    )


    # =========================================================================
    # Task 2: Generate Synthetic Data
    # =========================================================================

    generate_data = BashOperator(

        task_id="generate_synthetic_data",

        bash_command=(
            "cd /opt && "
            "python ingestion/generate_data.py"
        ),

        sla=timedelta(
            minutes=20
        ),
    )


    # =========================================================================
    # Task 3: Upload Raw Data to AWS S3
    # =========================================================================

    upload_s3 = BashOperator(

        task_id="upload_to_s3",

        bash_command=(
            "cd /opt && "
            "python ingestion/upload_to_s3.py"
        ),

        sla=timedelta(
            minutes=10
        ),

        env={
            # AWS credentials are supplied by Docker/Airflow
            "AWS_ACCESS_KEY_ID": os.getenv(
                "AWS_ACCESS_KEY_ID",
                "",
            ),

            "AWS_SECRET_ACCESS_KEY": os.getenv(
                "AWS_SECRET_ACCESS_KEY",
                "",
            ),

            "AWS_DEFAULT_REGION": AWS_REGION,

            # Bucket used by upload_to_s3.py
            "S3_BUCKET": S3_BUCKET,
        },
    )


    # =========================================================================
    # Task 4: Run Spark ETL
    # =========================================================================

    spark_etl = BashOperator(

        task_id="spark_etl_orders",

        bash_command=(
            "cd /opt && "
            "python spark_jobs/etl_orders.py"
        ),

        sla=timedelta(
            minutes=45
        ),

        env={
            # AWS credentials
            "AWS_ACCESS_KEY_ID": os.getenv(
                "AWS_ACCESS_KEY_ID",
                "",
            ),

            "AWS_SECRET_ACCESS_KEY": os.getenv(
                "AWS_SECRET_ACCESS_KEY",
                "",
            ),

            "AWS_DEFAULT_REGION": AWS_REGION,

            # Bucket used by Spark
            "S3_BUCKET": S3_BUCKET,
        },
    )


    # =========================================================================
    # Task 5: Data Quality Gate
    # =========================================================================

    data_quality = BashOperator(

        task_id="great_expectations_quality_gate",

        bash_command=(
            "cd /opt && "
            "python tests/test_data_quality.py"
        ),

        env={
            "AWS_ACCESS_KEY_ID": os.getenv(
                "AWS_ACCESS_KEY_ID",
                "",
            ),

            "AWS_SECRET_ACCESS_KEY": os.getenv(
                "AWS_SECRET_ACCESS_KEY",
                "",
            ),

            "AWS_DEFAULT_REGION": AWS_REGION,

            "S3_BUCKET": S3_BUCKET,
        },
    )


    # =========================================================================
    # Task 6: Log S3 Metrics
    # =========================================================================

    log_metrics = PythonOperator(

        task_id="log_s3_metrics",

        python_callable=log_s3_metrics,
    )


    # =========================================================================
    # Task 7: Trigger Transform DAG
    # =========================================================================

    trigger_transform = TriggerDagRunOperator(

        task_id="trigger_transform_dag",

        trigger_dag_id="retail_transform_dag",

        wait_for_completion=False,

        trigger_rule=TriggerRule.ALL_SUCCESS,
    )


    # =========================================================================
    # Task Dependencies
    # =========================================================================

    (
        check_freshness
        >> generate_data
        >> upload_s3
        >> spark_etl
        >> data_quality
        >> log_metrics
        >> trigger_transform
    )