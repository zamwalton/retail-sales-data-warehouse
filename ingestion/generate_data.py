"""
generate_data.py
────────────────
Generates 1 million synthetic retail orders with realistic
distributions, intentional nulls (~3%), and duplicates (~1%)
to simulate production-grade messy data.

Run: python ingestion/generate_data.py
"""

import csv
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────
random.seed(42)
Faker.seed(42)
fake = Faker()

OUTPUT_DIR = Path("data/raw")
TARGET_ROWS = 1_000_000

CATEGORIES = ["Electronics", "Clothing", "Food & Beverage", "Sports", "Home & Garden", "Books", "Toys"]
STATUSES   = ["completed", "returned", "pending", "cancelled", "processing"]
COUNTRIES  = ["IN", "US", "GB", "DE", "AU", "CA", "SG", "AE", "FR", "JP"]

PRODUCT_PRICE_RANGES = {
    "Electronics":     (50.00,  2000.00),
    "Clothing":        (10.00,   300.00),
    "Food & Beverage": (2.00,     80.00),
    "Sports":          (15.00,   500.00),
    "Home & Garden":   (8.00,    400.00),
    "Books":           (5.00,     60.00),
    "Toys":            (5.00,    200.00),
}

STATUS_WEIGHTS = [0.65, 0.10, 0.12, 0.08, 0.05]   # completed dominant


def random_date(start: str = "2023-01-01", end: str = "2024-12-31") -> str:
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    delta = random.randint(0, (e - s).days)
    return (s + timedelta(days=delta)).strftime("%Y-%m-%d")


def make_row() -> dict:
    category = random.choice(CATEGORIES)
    lo, hi   = PRODUCT_PRICE_RANGES[category]
    qty      = random.randint(1, 10)
    price    = round(random.uniform(lo, hi), 2)

    return {
        "order_id":     str(uuid.uuid4()),
        "customer_id":  f"C{random.randint(1, 50_000):06d}",
        "product_id":   f"P{random.randint(1, 5_000):05d}",
        "store_id":     f"S{random.randint(1, 200):03d}",
        "category":     category,
        "quantity":     qty,
        "unit_price":   price,
        "discount_pct": random.choice([0, 5, 10, 15, 20]),
        "order_date":   random_date(),
        "ship_date":    random_date("2023-01-02", "2025-01-15"),
        "status":       random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
        "country":      random.choice(COUNTRIES),
        "channel":      random.choice(["online", "in-store", "mobile"]),
        "payment":      random.choice(["credit_card", "upi", "net_banking", "cod"]),
    }


def inject_noise(row: dict) -> dict:
    """Inject ~3% nulls and corrupt ~0.5% dates to simulate real data."""
    nullable_cols = ["category", "country", "ship_date", "discount_pct"]
    if random.random() < 0.03:
        row[random.choice(nullable_cols)] = None
    if random.random() < 0.005:
        row["order_date"] = "9999-99-99"   # bad date — Spark will catch
    if random.random() < 0.002:
        row["unit_price"] = -abs(row["unit_price"])  # negative price — GE will catch
    return row


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = OUTPUT_DIR / "orders.csv"

    logger.info(f"Generating {TARGET_ROWS:,} rows → {out_file}")
    rows: list[dict] = []

    for i in range(TARGET_ROWS):
        row = inject_noise(make_row())
        rows.append(row)
        # Inject ~1% duplicates
        if random.random() < 0.01:
            rows.append(row.copy())

    fieldnames = list(rows[0].keys())
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    size_mb = out_file.stat().st_size / 1024 / 1024
    logger.success(f"Done — {len(rows):,} rows written ({size_mb:.1f} MB)")
    logger.info(f"Approx duplicates injected: {int(TARGET_ROWS * 0.01):,}")
    logger.info(f"Approx nulls injected:      {int(TARGET_ROWS * 0.03):,}")


if __name__ == "__main__":
    main()
