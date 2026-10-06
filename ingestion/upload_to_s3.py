"""
upload_to_s3.py
───────────────
Creates the S3 bucket structure and uploads raw CSV files.
Works with LocalStack (dev) and real AWS S3 (prod) via env vars.

Run: python ingestion/upload_to_s3.py
"""

import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
S3_ENDPOINT   = os.getenv("S3_ENDPOINT_URL", "").strip()
AWS_KEY       = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET    = os.getenv("AWS_SECRET_ACCESS_KEY", "")
REGION        = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BUCKET        = os.getenv("S3_BUCKET", "retail-pipeline-zam-2026")
RAW_FILES_DIR = Path("data/raw")


def get_s3_client():
    kwargs = {
        "aws_access_key_id": AWS_KEY,
        "aws_secret_access_key": AWS_SECRET,
        "region_name": REGION,
    }
    if S3_ENDPOINT:
        kwargs["endpoint_url"] = S3_ENDPOINT
    return boto3.client("s3", **kwargs)



def ensure_bucket(s3, bucket: str) -> None:
    """Verify that the configured S3 bucket exists and is accessible."""
    try:
        s3.head_bucket(Bucket=bucket)
        logger.info(f"Bucket '{bucket}' exists and is accessible")
    except ClientError as exc:
        logger.error(
            f"Cannot access S3 bucket '{bucket}'. "
            "Check the bucket name, AWS credentials, and IAM permissions."
        )
        raise


def upload_file(s3, local_path: Path, bucket: str, s3_key: str) -> None:
    file_size_mb = local_path.stat().st_size / 1024 / 1024
    logger.info(f"Uploading {local_path.name} ({file_size_mb:.1f} MB) → s3://{bucket}/{s3_key}")
    s3.upload_file(str(local_path), bucket, s3_key)
    logger.success(f"Upload complete: s3://{bucket}/{s3_key}")


def list_bucket(s3, bucket: str) -> None:
    resp = s3.list_objects_v2(Bucket=bucket)
    logger.info("Bucket contents:")
    for obj in resp.get("Contents", []):
        size_mb = obj["Size"] / 1024 / 1024
        logger.info(f"  {obj['Key']}  ({size_mb:.1f} MB)  last modified: {obj['LastModified']}")


def main() -> None:
    s3 = get_s3_client()
    ensure_bucket(s3, BUCKET)

    csv_files = list(RAW_FILES_DIR.glob("*.csv"))
    if not csv_files:
        logger.error(f"No CSV files found in {RAW_FILES_DIR}. Run generate_data.py first.")
        raise SystemExit(1)

    for csv_file in csv_files:
        s3_key = f"raw/orders/{csv_file.name}"
        upload_file(s3, csv_file, BUCKET, s3_key)

    list_bucket(s3, BUCKET)


if __name__ == "__main__":
    main()
