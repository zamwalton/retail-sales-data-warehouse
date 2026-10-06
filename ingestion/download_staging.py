"""
download_staging.py
─────────────────────
Downloads the Spark-written Parquet staging files from S3 (LocalStack)
to local disk, so test_data_quality.py can read them with pandas.

Run: python ingestion/download_staging.py
"""

import os
from pathlib import Path

import boto3
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "").strip()
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BUCKET = os.getenv("S3_BUCKET", "retail-pipeline-zam-2026")
S3_PREFIX = "staging/orders/"
LOCAL_DIR = Path("data/staging/orders")


def get_s3_client():
    kwargs = {
        "aws_access_key_id": AWS_KEY,
        "aws_secret_access_key": AWS_SECRET,
        "region_name": REGION,
    }
    if S3_ENDPOINT:
        kwargs["endpoint_url"] = S3_ENDPOINT
        logger.info(f"Using custom endpoint (LocalStack): {S3_ENDPOINT}")
    else:
        logger.info("Using real AWS S3 (no custom endpoint)")
    return boto3.client("s3", **kwargs)


def main() -> None:
    s3 = get_s3_client()
    if LOCAL_DIR.exists():
        import shutil

        shutil.rmtree(LOCAL_DIR)
        logger.info(f"Cleared stale local directory: {LOCAL_DIR}")

    LOCAL_DIR.mkdir(parents=True, exist_ok=True)

    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue  # skip folder markers

            rel_path = key[len(S3_PREFIX) :]
            local_path = LOCAL_DIR / rel_path
            local_path.parent.mkdir(parents=True, exist_ok=True)

            s3.download_file(BUCKET, key, str(local_path))
            count += 1

    if count == 0:
        logger.error(f"No files found at s3://{BUCKET}/{S3_PREFIX}")
        logger.error("Run spark_jobs/etl_orders.py first.")
        raise SystemExit(1)

    logger.success(f"Downloaded {count} file(s) → {LOCAL_DIR}")


if __name__ == "__main__":
    main()
