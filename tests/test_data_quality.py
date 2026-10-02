"""
test_data_quality.py
─────────────────────
Great Expectations data quality suite.
Acts as the DATA CONTRACT gate — runs after Spark ETL,
BEFORE any data reaches Snowflake or dbt.

If ANY expectation fails → pipeline halts (exit code 1).

Run: python tests/test_data_quality.py
"""

import sys
from pathlib import Path

import great_expectations as ge
import pandas as pd
from loguru import logger

STAGING_DIR = Path("data/staging/orders")


def load_data(path: Path) -> pd.DataFrame:
    """Load all parquet partitions into a single DataFrame."""
    if not path.exists():
        logger.error(f"Staging directory not found: {path}")
        logger.error("Run spark_jobs/etl_orders.py first.")
        sys.exit(1)

    parquet_files = list(path.rglob("*.parquet"))
    if not parquet_files:
        logger.error("No Parquet files found. ETL may have failed.")
        sys.exit(1)

    logger.info(f"Loading {len(parquet_files)} Parquet partition(s)...")
    df = pd.read_parquet(path)
    logger.info(f"Loaded {len(df):,} rows for validation")
    return df


def build_expectations(gdf) -> list[tuple[str, dict, str]]:
    """
    Returns list of (name, result_dict, severity).
    severity = 'critical' | 'warning'
    """
    expectations = []

    # ── Critical: Data Completeness ─────────────────────────────────────────
    for col in ["order_id", "customer_id", "product_id", "order_date", "unit_price"]:
        r = gdf.expect_column_values_to_not_be_null(col)
        expectations.append((f"{col}: no nulls", r, "critical"))

    # ── Critical: Uniqueness ────────────────────────────────────────────────
    r = gdf.expect_column_values_to_be_unique("order_id")
    expectations.append(("order_id: globally unique", r, "critical"))

    # ── Critical: Volume ────────────────────────────────────────────────────
    r = gdf.expect_table_row_count_to_be_between(800_000, 1_200_000)
    expectations.append(("row count: within expected range", r, "critical"))

    # ── Critical: Business Rules ─────────────────────────────────────────────
    r = gdf.expect_column_values_to_be_between("unit_price", min_value=0.01, max_value=10_000)
    expectations.append(("unit_price: positive and bounded", r, "critical"))

    r = gdf.expect_column_values_to_be_between("quantity", min_value=1, max_value=100)
    expectations.append(("quantity: valid range 1–100", r, "critical"))

    r = gdf.expect_column_values_to_be_between("net_amount", min_value=0.01)
    expectations.append(("net_amount: always positive", r, "critical"))

    r = gdf.expect_column_values_to_be_in_set(
        "status", ["completed", "returned", "pending", "cancelled", "processing", "unknown"]
    )
    expectations.append(("status: accepted values only", r, "critical"))

    # ── Critical: Schema ────────────────────────────────────────────────────
    required_cols = [
        "order_id", "customer_id", "product_id", "store_id", "category",
        "quantity", "unit_price", "discount_pct", "order_date", "status",
        "country", "channel", "payment", "gross_amount", "net_amount",
        "year", "month", "quarter"
    ]
    r = gdf.expect_table_columns_to_match_set(required_cols, exact_match=False)
    expectations.append(("schema: all required columns present", r, "critical"))

    # ── Warning: Distribution ────────────────────────────────────────────────
    r = gdf.expect_column_proportion_of_unique_values_to_be_between(
        "customer_id", min_value=0.01, max_value=0.20
    )
    expectations.append(("customer_id: cardinality in expected range", r, "warning"))

    r = gdf.expect_column_values_to_not_be_null("category", mostly=0.95)
    expectations.append(("category: at least 95% populated", r, "warning"))

    # ── Warning: Referential ─────────────────────────────────────────────────
    r = gdf.expect_column_values_to_match_regex(
        "customer_id", regex=r"^C\d{6}$"
    )
    expectations.append(("customer_id: format C######", r, "warning"))

    r = gdf.expect_column_values_to_match_regex(
        "product_id", regex=r"^P\d{5}$"
    )
    expectations.append(("product_id: format P#####", r, "warning"))

    return expectations


def run_suite(df: pd.DataFrame) -> bool:
    gdf = ge.from_pandas(df)
    expectations = build_expectations(gdf)

    passed = 0
    failed_critical = []
    failed_warnings = []

    max_name_len = max(len(name) for name, _, _ in expectations)

    logger.info("\n" + "─" * 60)
    logger.info("  DATA QUALITY REPORT")
    logger.info("─" * 60)

    for name, result, severity in expectations:
        success = result["success"]
        pct = result.get("result", {}).get("unexpected_percent", 0) or 0

        if success:
            passed += 1
            logger.info(f"  PASS  {name}")
        else:
            icon = "CRIT" if severity == "critical" else "WARN"
            logger.warning(f"  {icon}  {name}  ({pct:.2f}% unexpected)")
            if severity == "critical":
                failed_critical.append(name)
            else:
                failed_warnings.append(name)

    total = len(expectations)
    logger.info("─" * 60)
    logger.info(f"  Results: {passed}/{total} passed")

    if failed_warnings:
        logger.warning(f"  Warnings ({len(failed_warnings)}): {', '.join(failed_warnings)}")

    if failed_critical:
        logger.error(f"  CRITICAL FAILURES ({len(failed_critical)}):")
        for name in failed_critical:
            logger.error(f"    • {name}")
        logger.error("  ❌ QUALITY GATE FAILED — pipeline halted")
        return False

    logger.success("  ✅ ALL CRITICAL CHECKS PASSED — pipeline cleared")
    return True


def main() -> None:
    df       = load_data(STAGING_DIR)
    all_pass = run_suite(df)
    if not all_pass:
        sys.exit(1)
    logger.info("Data quality validation complete. Safe to proceed to dbt.")


if __name__ == "__main__":
    main()
