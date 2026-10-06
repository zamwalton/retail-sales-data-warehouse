"""
test_data_quality.py
--------------------
Data quality gate for the retail pipeline.

Validates the cleaned Parquet dataset written by Spark to AWS S3.
Critical failures stop the pipeline with exit code 1.
Warning failures are reported but do not stop the pipeline.

Run from the project root:
    python tests/test_data_quality.py
"""

import os
import sys

from loguru import logger
from pyspark.sql import SparkSession, functions as F

S3_BUCKET = os.getenv("S3_BUCKET", "retail-pipeline-zam-2026")
STAGING_PATH = f"s3a://{S3_BUCKET}/staging/orders/"

REQUIRED_COLUMNS = [
    "order_id", "customer_id", "product_id", "store_id", "category",
    "quantity", "unit_price", "discount_pct", "order_date", "status",
    "country", "channel", "payment", "gross_amount", "net_amount",
    "year", "month", "quarter",
]

ACCEPTED_STATUSES = [
    "completed", "returned", "pending",
    "cancelled", "processing", "unknown",
]


def create_spark() -> SparkSession:
    """Create Spark with the same S3A dependencies as the ETL job."""
    builder = (
        SparkSession.builder
        .appName("RetailDataQualityGate")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
    )

    return builder.getOrCreate()


def evaluate(df, name, condition, severity="critical"):
    """Evaluate a condition where matching rows are considered invalid."""
    invalid_count = df.filter(condition).count()
    total_count = df.count()
    success = invalid_count == 0
    pct = (invalid_count / total_count * 100) if total_count else 0.0

    if success:
        logger.info(f"PASS  {name}")
    else:
        level = "CRIT" if severity == "critical" else "WARN"
        logger.warning(
            f"{level}  {name} ({invalid_count:,} invalid rows; "
            f"{pct:.2f}% of dataset)"
        )

    return {
        "name": name,
        "success": success,
        "severity": severity,
        "invalid_count": invalid_count,
        "unexpected_percent": pct,
    }


def run_suite(df) -> bool:
    """Run data-contract checks against the Spark DataFrame."""
    logger.info("Loading quality checks for the S3 staging dataset")

    total = df.count()
    logger.info(f"Rows available for validation: {total:,}")

    if total == 0:
        logger.error("CRITICAL: staging dataset is empty")
        return False

    results = []

    # Critical: required fields
    for col_name in [
        "order_id", "customer_id", "product_id",
        "order_date", "unit_price",
    ]:
        results.append(evaluate(
            df,
            f"{col_name}: no nulls",
            F.col(col_name).isNull(),
        ))

    # Critical: unique order IDs
    duplicate_count = (
        df.groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    duplicate_result = {
        "name": "order_id: globally unique",
        "success": duplicate_count == 0,
        "severity": "critical",
        "invalid_count": duplicate_count,
        "unexpected_percent": duplicate_count / total * 100,
    }
    results.append(duplicate_result)

    if duplicate_result["success"]:
        logger.info("PASS  order_id: globally unique")
    else:
        logger.warning(
            f"CRIT  order_id: globally unique "
            f"({duplicate_count:,} duplicate IDs)"
        )

    # Critical: expected data volume
    volume_ok = 800_000 <= total <= 1_200_000
    results.append({
        "name": "row count: within expected range",
        "success": volume_ok,
        "severity": "critical",
        "invalid_count": 0 if volume_ok else total,
        "unexpected_percent": 0.0 if volume_ok else 100.0,
    })
    logger.info(
        f"{'PASS' if volume_ok else 'CRIT'}  "
        f"row count: {total:,} "
        f"(expected 800,000–1,200,000)"
    )

    # Critical: business rules
    results.append(evaluate(
        df,
        "unit_price: positive and bounded",
        (F.col("unit_price") < 0.01)
        | (F.col("unit_price") > 10_000)
        | F.col("unit_price").isNull(),
    ))

    results.append(evaluate(
        df,
        "quantity: valid range 1–100",
        (F.col("quantity") < 1)
        | (F.col("quantity") > 100)
        | F.col("quantity").isNull(),
    ))

    results.append(evaluate(
        df,
        "net_amount: always positive",
        (F.col("net_amount") < 0.01)
        | F.col("net_amount").isNull(),
    ))

    results.append(evaluate(
        df,
        "status: accepted values only",
        ~F.col("status").isin(ACCEPTED_STATUSES)
        | F.col("status").isNull(),
    ))

    # Critical: required schema
    missing_columns = sorted(set(REQUIRED_COLUMNS) - set(df.columns))
    schema_ok = not missing_columns
    results.append({
        "name": "schema: all required columns present",
        "success": schema_ok,
        "severity": "critical",
        "invalid_count": len(missing_columns),
        "unexpected_percent": 0.0 if schema_ok else 100.0,
    })

    if schema_ok:
        logger.info("PASS  schema: all required columns present")
    else:
        logger.warning(f"CRIT  missing columns: {missing_columns}")

    # Warning: customer ID cardinality
    distinct_customers = df.select("customer_id").distinct().count()
    cardinality = distinct_customers / total
    cardinality_ok = 0.01 <= cardinality <= 0.20
    results.append({
        "name": "customer_id: cardinality in expected range",
        "success": cardinality_ok,
        "severity": "warning",
        "invalid_count": 0 if cardinality_ok else distinct_customers,
        "unexpected_percent": 0.0 if cardinality_ok else 100.0,
    })
    logger.info(
        f"{'PASS' if cardinality_ok else 'WARN'}  "
        f"customer_id cardinality: {cardinality:.2%}"
    )

    # Warning: category completeness (at least 95%)
    null_categories = df.filter(
        F.col("category").isNull()
        | (F.trim(F.col("category")) == "")
    ).count()
    category_completeness = 1 - null_categories / total
    category_ok = category_completeness >= 0.95
    results.append({
        "name": "category: at least 95% populated",
        "success": category_ok,
        "severity": "warning",
        "invalid_count": null_categories,
        "unexpected_percent": null_categories / total * 100,
    })
    logger.info(
        f"{'PASS' if category_ok else 'WARN'}  "
        f"category completeness: {category_completeness:.2%}"
    )

    # Warning: ID formats
    results.append(evaluate(
        df,
        "customer_id: format C######",
        ~F.col("customer_id").rlike(r"^C\d{6}$")
        | F.col("customer_id").isNull(),
        severity="warning",
    ))

    results.append(evaluate(
        df,
        "product_id: format P#####",
        ~F.col("product_id").rlike(r"^P\d{5}$")
        | F.col("product_id").isNull(),
        severity="warning",
    ))

    failed_critical = [
        r["name"] for r in results
        if not r["success"] and r["severity"] == "critical"
    ]
    failed_warnings = [
        r["name"] for r in results
        if not r["success"] and r["severity"] == "warning"
    ]

    passed = sum(r["success"] for r in results)

    logger.info("=" * 60)
    logger.info(f"DATA QUALITY RESULTS: {passed}/{len(results)} passed")

    if failed_warnings:
        logger.warning(f"Warnings: {', '.join(failed_warnings)}")

    if failed_critical:
        logger.error(f"CRITICAL FAILURES: {', '.join(failed_critical)}")
        logger.error("QUALITY GATE FAILED — pipeline halted")
        return False

    logger.success("ALL CRITICAL CHECKS PASSED — pipeline cleared")
    return True


def main():
    spark = None
    try:
        if not S3_BUCKET:
            logger.error("S3_BUCKET is not configured")
            sys.exit(1)

        logger.info(f"Validating S3 staging data: {STAGING_PATH}")
        spark = create_spark()

        df = spark.read.parquet(STAGING_PATH)

        missing = sorted(set(REQUIRED_COLUMNS) - set(df.columns))
        if missing:
            logger.error(f"Required columns missing: {missing}")
            sys.exit(1)

        if not run_suite(df):
            sys.exit(1)

        logger.success("Data quality validation complete")
    except Exception:
        logger.exception("Data quality gate encountered an error")
        sys.exit(1)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()

