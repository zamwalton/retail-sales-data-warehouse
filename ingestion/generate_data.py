"""
generate_data.py
----------------
Generates synthetic retail orders with realistic distributions,
intentional nulls, duplicates, invalid dates, and negative prices.

Rows are streamed directly to CSV to reduce memory consumption.

Run: python ingestion/generate_data.py
"""

import csv
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker
from loguru import logger

# Configuration
random.seed(42)
Faker.seed(42)
fake = Faker()

OUTPUT_DIR = Path("data/raw")
TARGET_ROWS = 1_000_000

CATEGORIES = [
    "Electronics",
    "Clothing",
    "Food & Beverage",
    "Sports",
    "Home & Garden",
    "Books",
    "Toys",
]
STATUSES = [
    "completed",
    "returned",
    "pending",
    "cancelled",
    "processing",
]
COUNTRIES = ["IN", "US", "GB", "DE", "AU", "CA", "SG", "AE", "FR", "JP"]

PRODUCT_PRICE_RANGES = {
    "Electronics": (50.00, 2000.00),
    "Clothing": (10.00, 300.00),
    "Food & Beverage": (2.00, 80.00),
    "Sports": (15.00, 500.00),
    "Home & Garden": (8.00, 400.00),
    "Books": (5.00, 60.00),
    "Toys": (5.00, 200.00),
}

STATUS_WEIGHTS = [0.65, 0.10, 0.12, 0.08, 0.05]

FIELDNAMES = [
    "order_id",
    "customer_id",
    "product_id",
    "store_id",
    "category",
    "quantity",
    "unit_price",
    "discount_pct",
    "order_date",
    "ship_date",
    "status",
    "country",
    "channel",
    "payment",
]


def random_date(
    start: str = "2023-01-01",
    end: str = "2024-12-31",
) -> str:
    start_date = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_date = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    delta_days = (end_date - start_date).days
    return (start_date + timedelta(days=random.randint(0, delta_days))).strftime(
        "%Y-%m-%d"
    )


def make_row() -> dict:
    category = random.choice(CATEGORIES)
    low_price, high_price = PRODUCT_PRICE_RANGES[category]
    quantity = random.randint(1, 10)
    unit_price = round(random.uniform(low_price, high_price), 2)

    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"C{random.randint(1, 50_000):06d}",
        "product_id": f"P{random.randint(1, 5_000):05d}",
        "store_id": f"S{random.randint(1, 200):03d}",
        "category": category,
        "quantity": quantity,
        "unit_price": unit_price,
        "discount_pct": random.choice([0, 5, 10, 15, 20]),
        "order_date": random_date(),
        "ship_date": random_date("2023-01-02", "2025-01-15"),
        "status": random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
        "country": random.choice(COUNTRIES),
        "channel": random.choice(["online", "in-store", "mobile"]),
        "payment": random.choice(["credit_card", "upi", "net_banking", "cod"]),
    }


def inject_noise(row: dict) -> dict:
    """Inject nulls, invalid dates, and negative prices for testing."""
    if random.random() < 0.03:
        row[random.choice(["category", "country", "ship_date", "discount_pct"])] = None

    if random.random() < 0.005:
        row["order_date"] = "9999-99-99"

    if random.random() < 0.002:
        row["unit_price"] = -abs(row["unit_price"])

    return row


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_DIR / "orders.csv"
    duplicate_count = 0

    logger.info(f"Generating {TARGET_ROWS:,} base rows -> {output_file}")

    # Stream rows directly to disk instead of retaining them in a list.
    with output_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
        writer.writeheader()

        for index in range(TARGET_ROWS):
            row = inject_noise(make_row())
            writer.writerow(row)

            # Preserve the original behavior: duplicate the same row
            # with approximately 1% probability.
            if random.random() < 0.01:
                writer.writerow(row.copy())
                duplicate_count += 1

            if (index + 1) % 100_000 == 0:
                logger.info(f"Generated {index + 1:,}/{TARGET_ROWS:,} base rows")

    size_mb = output_file.stat().st_size / (1024 * 1024)

    logger.success(
        f"Finished: {TARGET_ROWS + duplicate_count:,} total rows " f"({size_mb:.1f} MB)"
    )
    logger.info(f"Additional duplicate rows: {duplicate_count:,}")
    logger.info("Output file: {}", output_file.resolve())


if __name__ == "__main__":
    main()
