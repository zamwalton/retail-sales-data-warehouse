"""
retail_ingestion_dag.py
──────────────────────────────────────────────────────────────────────────────
DAG 1: Raw data ingestion pipeline.

Runs daily at 01:00 UTC.
Triggers the transform DAG after successful ingestion and data-quality checks.

Pipeline:

    1. Generate synthetic source data
    2. Upload raw data to AWS S3
    3. Validate raw data in AWS S3
    4. Run Spark ETL
    5. Run data-quality gate
    6. Log S3 metrics
    7. Load Snowflake RAW layer
    8. Trigger transform DAG

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
# Task 1: Validate raw data in AWS S3
# =============================================================================

def validate_raw_s3(**context) -> None:
    """
    Validate that the expected raw source file was successfully
    uploaded to AWS S3.

    Checks:
        1. Expected object exists.
        2. Object is not empty.
        3. Reports object size and last-modified timestamp.

    This is an ingestion validation rather than a source-freshness
    check because the current pipeline generates the source data
    itself.
    """

    s3 = get_s3_client()

    raw_key = f"{RAW_PREFIX}orders.csv"

    try:
        response = s3.head_object(
            Bucket=S3_BUCKET,
            Key=raw_key,
        )
    except ClientError as exc:
        error_code = exc.response.get(
            "Error",
            {},
        ).get(
            "Code",
            "",
        )

        if error_code in ("404", "NoSuchKey", "NotFound"):
            raise FileNotFoundError(
                f"Raw source file was not found: "
                f"s3://{S3_BUCKET}/{raw_key}"
            ) from exc

        raise

    object_size = response.get(
        "ContentLength",
        0,
    )

    last_modified = response.get(
        "LastModified",
    )

    print("=" * 80)
    print("AWS S3 RAW INGESTION VALIDATION")
    print(f"Bucket        : {S3_BUCKET}")
    print(f"Object        : {raw_key}")
    print(f"Size          : {object_size:,} bytes")
    print(f"Last modified : {last_modified}")
    print("=" * 80)

    if object_size <= 0:
        raise ValueError(
            f"Raw S3 object is empty: "
            f"s3://{S3_BUCKET}/{raw_key}"
        )

    print("Raw S3 ingestion validation PASSED.")


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

    synthetic data generation
    ↓
    s3 upload
    ↓
    S3 upload validation
    ↓
    Spark transformation
    ↓
    Data quality validation
    ↓
    S3 metrics
    ↓
    Snowflake RAW load
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
    # Task 1: Validate raw data in AWS S3
    # =========================================================================

    validate_raw = PythonOperator(

        task_id="validate_raw_s3",

        python_callable=validate_raw_s3,
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
        append_env=True,

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
        append_env=True,

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

        append_env=True,
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



    # =============================================================================
    # Task 7: Load Snowflake RAW layer
    # =============================================================================

    snowflake_raw_load = BashOperator(
        task_id="load_snowflake_raw",
        bash_command=(
            "cd /opt && "
            "python ingestion/load_snowflake_raw.py"
        ),
        sla=timedelta(
            minutes=30
        ),
    )


    # =========================================================================
    # Task 8: Trigger Transform DAG
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
        generate_data
        >> upload_s3
        >> validate_raw
        >> spark_etl
        >> data_quality
        >> log_metrics
        >> snowflake_raw_load
        >> trigger_transform
    )