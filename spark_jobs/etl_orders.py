"""
etl_orders.py
─────────────
PySpark ETL: reads raw orders CSV from S3, applies cleaning
and enrichment transformations, writes partitioned Parquet to
the staging layer.

Medallion Architecture:
  Bronze (S3 raw/)  →  Silver (S3 staging/)  →  Gold (Snowflake RAW)

Run: python spark_jobs/etl_orders.py
"""
import os

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

import sys
from datetime import date
from dotenv import load_dotenv
load_dotenv()

from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, StringType, StructField, StructType,
)

# ── Config ────────────────────────────────────────────────────────────────────
S3_ENDPOINT   = os.getenv("S3_ENDPOINT_URL", "").strip()
S3_BUCKET     = os.getenv("S3_BUCKET", "retail-pipeline-zam-2026")
RAW_S3_PATH   = f"s3a://{S3_BUCKET}/raw/orders/"
STAGE_S3_PATH = f"s3a://{S3_BUCKET}/staging/orders/"

VALID_STATUSES   = {"completed", "returned", "pending", "cancelled", "processing"}
VALID_CATEGORIES = {"Electronics", "Clothing", "Food & Beverage", "Sports",
                    "Home & Garden", "Books", "Toys"}


# ── Config Validation ─────────────────────────────────────────────────────────
def validate_config() -> None:
    """Fail fast with a clear message if config looks wrong, before Spark even starts."""
    errors = []

    if S3_BUCKET in ("retail-pipeline", ""):
        errors.append(
            f"S3_BUCKET is '{S3_BUCKET}' — looks like the default, not your real bucket. "
            "Check .env has S3_BUCKET set correctly and you're running from the project root."
        )

    key = os.getenv("AWS_ACCESS_KEY_ID", "")
    if not key:
        errors.append("AWS_ACCESS_KEY_ID is not set in .env")
    elif len(key) != 20 or not key.startswith("AKIA"):
        errors.append(
            f"AWS_ACCESS_KEY_ID looks malformed (length={len(key)}, expected 20, starts with AKIA). "
            "Check for typos or a leftover old key in .env"
        )

    secret = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    if not secret:
        errors.append("AWS_SECRET_ACCESS_KEY is not set in .env")
    elif len(secret) != 40:
        errors.append(f"AWS_SECRET_ACCESS_KEY looks malformed (length={len(secret)}, expected 40)")

    if errors:
        logger.error("─── Config validation failed ───────────────────────")
        for e in errors:
            logger.error(f"  ✗ {e}")
        logger.error("──────────────────────────────────────────────────────")
        raise SystemExit(1)

    logger.success(f"Config OK — bucket: {S3_BUCKET}, key: {key[:6]}...{key[-4:]}")


# ── Spark Session ─────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("RetailOrdersETL")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "test"))
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.network.timeout", "300s")
    )

    if S3_ENDPOINT:
        # LocalStack — needs custom endpoint + path-style access
        builder = (builder
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.path.style.access", "true"))
    else:
        # Real AWS S3 — virtual-hosted-style (default), no custom endpoint
        builder = (builder
            .config("spark.hadoop.fs.s3a.path.style.access", "false")
            .config("spark.hadoop.fs.s3a.endpoint.region", os.getenv("AWS_DEFAULT_REGION", "us-east-1")))

    return builder.getOrCreate()


# ── Schema ────────────────────────────────────────────────────────────────────
RAW_SCHEMA = StructType([
    StructField("order_id",     StringType()),
    StructField("customer_id",  StringType()),
    StructField("product_id",   StringType()),
    StructField("store_id",     StringType()),
    StructField("category",     StringType()),
    StructField("quantity",     StringType()),
    StructField("unit_price",   StringType()),
    StructField("discount_pct", StringType()),
    StructField("order_date",   StringType()),
    StructField("ship_date",    StringType()),
    StructField("status",       StringType()),
    StructField("country",      StringType()),
    StructField("channel",      StringType()),
    StructField("payment",      StringType()),
])


# ── Extract ───────────────────────────────────────────────────────────────────
def extract(spark: SparkSession, path: str) -> DataFrame:
    logger.info(f"Reading raw data from: {path}")
    df = (spark.read
          .option("header", "true")
          .option("multiLine", "false")
          .schema(RAW_SCHEMA)
          .csv(path))
    raw_count = df.count()
    logger.info(f"Raw row count: {raw_count:,}")
    return df


# ── Transform ─────────────────────────────────────────────────────────────────
def transform(df: DataFrame) -> DataFrame:
    logger.info("Applying transformations...")

    df = (df
        .withColumn("quantity",     F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price",   F.col("unit_price").cast(DoubleType()))
        .withColumn("discount_pct", F.col("discount_pct").cast(IntegerType()))
        .withColumn("order_date",   F.to_date("order_date", "yyyy-MM-dd"))
        .withColumn("ship_date",    F.to_date("ship_date",  "yyyy-MM-dd"))
    )

    df = df.dropna(subset=["order_id", "customer_id", "order_date", "unit_price", "product_id"])

    df = df.filter(
        F.col("order_date").isNotNull() &
        (F.col("unit_price") > 0) &
        (F.col("quantity") > 0)
    )

    df = (df
        .withColumn("status",  F.lower(F.trim(F.col("status"))))
        .withColumn("channel", F.lower(F.trim(F.col("channel"))))
        .withColumn("payment", F.lower(F.trim(F.col("payment"))))
        .withColumn("country", F.upper(F.trim(F.col("country"))))
    )

    df = df.withColumn(
        "status",
        F.when(F.col("status").isin(list(VALID_STATUSES)), F.col("status"))
         .otherwise(F.lit("unknown"))
    )

    from pyspark.sql.window import Window
    w = Window.partitionBy("order_id").orderBy(F.col("order_date").desc())
    df = (df
        .withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    df = (df
        .withColumn("discount_amount",
            F.round(F.col("unit_price") * F.col("quantity") *
                    (F.col("discount_pct") / 100.0), 2))
        .withColumn("gross_amount",
            F.round(F.col("unit_price") * F.col("quantity"), 2))
        .withColumn("net_amount",
            F.round(F.col("unit_price") * F.col("quantity") *
                    (1 - F.col("discount_pct") / 100.0), 2))
        .withColumn("year",    F.year("order_date"))
        .withColumn("month",   F.month("order_date"))
        .withColumn("quarter", F.quarter("order_date"))
        .withColumn("day_of_week", F.dayofweek("order_date"))
        .withColumn("is_weekend",
            F.when(F.col("day_of_week").isin([1, 7]), True).otherwise(False))
        .withColumn("days_to_ship",
            F.datediff(F.col("ship_date"), F.col("order_date")))
        .withColumn("etl_loaded_at", F.current_timestamp())
    )

    return df


# ── Load to S3 Staging ────────────────────────────────────────────────────────
def load_to_staging(df: DataFrame, path: str) -> int:
    logger.info(f"Writing Parquet to staging: {path}")
    clean_count = df.count()
    (df.coalesce(4)
       .write
       .mode("overwrite")
       .partitionBy("year", "month")
       .parquet(path))
    logger.success(f"Written {clean_count:,} rows to staging")
    return clean_count


# ── Load to Snowflake (optional, unused — dbt/COPY INTO handles this) ────────
def load_to_snowflake(spark: SparkSession, df: DataFrame) -> None:
    snowflake_options = {
        "sfURL":       f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
        "sfUser":      os.getenv("SNOWFLAKE_USER"),
        "sfPassword":  os.getenv("SNOWFLAKE_PASSWORD"),
        "sfDatabase":  os.getenv("SNOWFLAKE_DATABASE", "RETAIL_DB"),
        "sfSchema":    os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
        "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "RETAIL_WH"),
        "sfRole":      "SYSADMIN",
    }
    logger.info("Writing to Snowflake RAW.orders...")
    (df.write
       .format("net.snowflake.spark.snowflake")
       .options(**snowflake_options)
       .option("dbtable", "orders")
       .mode("overwrite")
       .save())
    logger.success("Snowflake load complete")


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    validate_config()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_df   = extract(spark, RAW_S3_PATH)
    clean_df = transform(raw_df)

    raw_count   = raw_df.count()
    clean_count = load_to_staging(clean_df, STAGE_S3_PATH)

    logger.info("─── ETL Summary ───────────────────────────────────")
    logger.info(f"  Raw rows:        {raw_count:,}")
    logger.info(f"  Clean rows:      {clean_count:,}")
    logger.info(f"  Rows dropped:    {raw_count - clean_count:,}")
    logger.info(f"  Drop rate:       {(raw_count - clean_count)/raw_count*100:.1f}%")
    logger.info("────────────────────────────────────────────────────")

    spark.stop()


if __name__ == "__main__":
    main()