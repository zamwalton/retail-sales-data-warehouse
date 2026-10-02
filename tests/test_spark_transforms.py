"""
test_spark_transforms.py
─────────────────────────
Unit tests for PySpark transformation logic.
Uses chispa for DataFrame-level assertions.

Run: pytest tests/test_spark_transforms.py -v
"""

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType,
    StringType, StructField, StructType, TimestampType,
)

from spark_jobs.etl_orders import transform


@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .appName("RetailETLTests")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate())


@pytest.fixture
def raw_schema():
    return StructType([
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


def make_raw_df(spark, rows, schema):
    return spark.createDataFrame(rows, schema=schema)


class TestNullDropping:
    def test_drops_null_order_id(self, spark, raw_schema):
        rows = [
            ("ORD001", "C000001", "P00001", "S001", "Electronics", "2", "99.99",
             "0", "2024-01-15", "2024-01-17", "completed", "IN", "online", "upi"),
            (None,     "C000002", "P00002", "S001", "Clothing",    "1", "49.99",
             "0", "2024-01-16", "2024-01-18", "pending",   "US", "online", "credit_card"),
        ]
        df = make_raw_df(spark, rows, raw_schema)
        result = transform(df)
        assert result.filter(F.col("order_id").isNull()).count() == 0
        assert result.count() == 1

    def test_drops_null_order_date(self, spark, raw_schema):
        rows = [
            ("ORD001", "C000001", "P00001", "S001", "Electronics", "2", "99.99",
             "0", None, "2024-01-17", "completed", "IN", "online", "upi"),
        ]
        df = make_raw_df(spark, rows, raw_schema)
        result = transform(df)
        assert result.count() == 0

    def test_drops_negative_price(self, spark, raw_schema):
        rows = [
            ("ORD001", "C000001", "P00001", "S001", "Electronics", "2", "-50.00",
             "0", "2024-01-15", "2024-01-17", "completed", "IN", "online", "upi"),
        ]
        df = make_raw_df(spark, rows, raw_schema)
        result = transform(df)
        assert result.count() == 0


class TestDeduplication:
    def test_deduplicates_order_id(self, spark, raw_schema):
        rows = [
            ("ORD001", "C000001", "P00001", "S001", "Electronics", "2", "99.99",
             "0", "2024-01-15", "2024-01-17", "completed", "IN", "online", "upi"),
            ("ORD001", "C000001", "P00001", "S001", "Electronics", "2", "99.99",
             "0", "2024-01-15", "2024-01-17", "completed", "IN", "online", "upi"),
        ]
        df = make_raw_df(spark, rows, raw_schema)
        result = transform(df)
        assert result.count() == 1

    def test_keeps_all_unique_orders(self, spark, raw_schema):
        rows = [
            ("ORD001", "C000001", "P00001", "S001", "Electronics", "2", "99.99",
             "0", "2024-01-15", "2024-01-17", "completed", "IN", "online", "upi"),
            ("ORD002", "C000002", "P00002", "S001", "Clothing",    "1", "49.99",
             "10", "2024-01-16", "2024-01-18", "pending",   "US", "mobile", "credit_card"),
        ]
        df = make_raw_df(spark, rows, raw_schema)
        result = transform(df)
        assert result.count() == 2


class TestDerivedColumns:
    def test_gross_amount_calculation(self, spark, raw_schema):
        rows = [
            ("ORD001", "C000001", "P00001", "S001", "Electronics", "3", "100.00",
             "0", "2024-01-15", "2024-01-17", "completed", "IN", "online", "upi"),
        ]
        df = make_raw_df(spark, rows, raw_schema)
        result = transform(df)
        row = result.collect()[0]
        assert row["gross_amount"] == 300.0   # 3 * 100.00

    def test_net_amount_with_discount(self, spark, raw_schema):
        rows = [
            ("ORD001", "C000001", "P00001", "S001", "Electronics", "2", "100.00",
             "20", "2024-01-15", "2024-01-17", "completed", "IN", "online", "upi"),
        ]
        df = make_raw_df(spark, rows, raw_schema)
        result = transform(df)
        row = result.collect()[0]
        assert row["net_amount"] == 160.0   # 2 * 100.00 * (1 - 0.20)

    def test_year_month_quarter_extracted(self, spark, raw_schema):
        rows = [
            ("ORD001", "C000001", "P00001", "S001", "Electronics", "1", "50.00",
             "0", "2024-07-15", "2024-07-17", "completed", "IN", "online", "upi"),
        ]
        df = make_raw_df(spark, rows, raw_schema)
        result = transform(df)
        row = result.collect()[0]
        assert row["year"]    == 2024
        assert row["month"]   == 7
        assert row["quarter"] == 3

    def test_is_weekend_flag(self, spark, raw_schema):
        # 2024-01-13 is a Saturday
        rows = [
            ("ORD001", "C000001", "P00001", "S001", "Electronics", "1", "50.00",
             "0", "2024-01-13", None, "completed", "IN", "online", "upi"),
        ]
        df = make_raw_df(spark, rows, raw_schema)
        result = transform(df)
        row = result.collect()[0]
        assert row["is_weekend"] is True


class TestStatusStandardisation:
    def test_status_lowercased(self, spark, raw_schema):
        rows = [
            ("ORD001", "C000001", "P00001", "S001", "Electronics", "1", "50.00",
             "0", "2024-01-15", "2024-01-17", "COMPLETED", "IN", "online", "upi"),
        ]
        df = make_raw_df(spark, rows, raw_schema)
        result = transform(df)
        assert result.collect()[0]["status"] == "completed"

    def test_invalid_status_becomes_unknown(self, spark, raw_schema):
        rows = [
            ("ORD001", "C000001", "P00001", "S001", "Electronics", "1", "50.00",
             "0", "2024-01-15", "2024-01-17", "GARBAGE_STATUS", "IN", "online", "upi"),
        ]
        df = make_raw_df(spark, rows, raw_schema)
        result = transform(df)
        assert result.collect()[0]["status"] == "unknown"
