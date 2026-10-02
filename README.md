# Retail Sales Intelligence Pipeline

> End-to-end data engineering portfolio project built on the Deloitte enterprise stack.  
> 1M+ rows · PySpark · Snowflake · dbt · Apache Airflow · Great Expectations · Docker

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    MEDALLION ARCHITECTURE                         │
│                                                                   │
│  ┌──────────┐    ┌─────────────┐    ┌───────────┐    ┌───────┐  │
│  │  Python  │───▶│  S3 raw/    │───▶│  PySpark  │───▶│  S3   │  │
│  │  Faker   │    │  (Bronze)   │    │  ETL Job  │    │staging│  │
│  └──────────┘    └─────────────┘    └───────────┘    └───┬───┘  │
│                                                           │       │
│  ┌─────────────────────────────────────────────────────┐  │       │
│  │              QUALITY GATE (Great Expectations)      │◀─┘       │
│  │  • Null checks • Row count • Schema • Business rules │          │
│  └──────────────────────────┬──────────────────────────┘          │
│                             │ PASS                                 │
│                             ▼                                     │
│  ┌─────────────────────────────────────────────────────┐          │
│  │              SNOWFLAKE (Silver / Gold)               │          │
│  │  RAW.orders ──▶ STAGING.stg_orders (dbt view)       │          │
│  │                     │                                │          │
│  │           ┌─────────┼─────────┐                     │          │
│  │           ▼         ▼         ▼                     │          │
│  │     dim_customers dim_products dim_date              │          │
│  │           └─────────┬─────────┘                     │          │
│  │                     ▼                                │          │
│  │               fct_sales ──▶ Metabase Dashboard       │          │
│  └─────────────────────────────────────────────────────┘          │
│                                                                   │
│  Orchestrated by Apache Airflow DAGs  ·  CI via GitHub Actions    │
└─────────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Layer             | Tool                        | Version  |
|-------------------|-----------------------------|----------|
| Data Generation   | Python + Faker              | 3.11     |
| Object Storage    | AWS S3 / LocalStack (dev)   | 3.3      |
| Distributed ETL   | Apache Spark (PySpark)      | 3.5.1    |
| Data Quality      | Great Expectations          | 0.18.13  |
| Cloud Warehouse   | Snowflake                   | —        |
| Data Modelling    | dbt Core                    | 1.8.0    |
| Orchestration     | Apache Airflow              | 2.9.1    |
| Containerisation  | Docker Compose              | —        |
| BI Dashboard      | Metabase                    | 0.49.0   |
| CI/CD             | GitHub Actions              | —        |
| Testing           | pytest + chispa             | —        |

---

## Project Structure

```
retail-pipeline/
├── ingestion/
│   ├── generate_data.py          # 1M synthetic orders with realistic noise
│   └── upload_to_s3.py           # S3 bucket setup and file upload
│
├── spark_jobs/
│   └── etl_orders.py             # PySpark: raw CSV → clean Parquet
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   ├── sources.yml       # Source freshness tests
│       │   └── stg_orders.sql    # Clean, typed staging view
│       └── marts/
│           ├── schema.yml        # Column-level dbt tests
│           ├── dim_customers.sql # RFM-segmented customer dimension
│           ├── dim_products.sql  # Revenue-tiered product dimension
│           ├── dim_date.sql      # Calendar date dimension
│           └── fct_sales.sql     # Core fact table (star schema)
│
├── airflow/
│   └── dags/
│       ├── retail_ingestion_dag.py   # DAG 1: ingest → S3 → Spark → GE
│       └── retail_transform_dag.py   # DAG 2: dbt staging → dims → facts
│
├── tests/
│   ├── test_data_quality.py      # Great Expectations suite (14 checks)
│   └── test_spark_transforms.py  # pytest unit tests for PySpark logic
│
├── config/
│   └── snowflake_setup.sql       # Full Snowflake DDL + permissions
│
├── .github/
│   └── workflows/
│       └── ci.yml                # CI: lint → Spark tests → dbt compile
│
├── docker-compose.yml            # LocalStack + Airflow + Postgres + Metabase
├── requirements.txt
├── .env.example
└── README.md
```

---

## Quick Start (Day 1 — Local Dev)

### Prerequisites
- Python 3.11+
- Docker Desktop
- Java 11+ (for Spark)
- Snowflake free trial → https://signup.snowflake.com

### 1. Clone & setup

```bash
git clone https://github.com/YOUR_USERNAME/retail-pipeline.git
cd retail-pipeline

python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Fill in your Snowflake credentials in .env
```

### 2. Start infrastructure

```bash
docker-compose up -d
# Services:
#   Airflow UI  → http://localhost:8080  (admin/admin)
#   Metabase    → http://localhost:3000
#   LocalStack  → http://localhost:4566
```

### 3. Run Day 1 pipeline

```bash
# Generate data
python ingestion/generate_data.py

# Upload to S3
python ingestion/upload_to_s3.py

# Spark ETL
python spark_jobs/etl_orders.py

# Data quality gate
python tests/test_data_quality.py
```

### 4. Run Day 2 — Snowflake + dbt

```bash
# Setup Snowflake (run config/snowflake_setup.sql in Snowflake UI)

# Run dbt
cd dbt
dbt deps
dbt run --select staging
dbt test --select staging
dbt run --select marts
dbt test --select marts
dbt docs generate && dbt docs serve
```

### 5. Run tests

```bash
pytest tests/test_spark_transforms.py -v --cov=spark_jobs
```

---

## Data Model

### Star Schema

```
              dim_date
                 │
dim_customers ───┤
                 ├── fct_sales
dim_products  ───┤
```

### Key Metrics Available

- Revenue by country, channel, category (fct_sales)
- Customer LTV and RFM segments (dim_customers)
- Product return rates and revenue tiers (dim_products)
- Time-series analysis with full calendar grain (dim_date)

---

## Design Decisions

**Why PySpark over Pandas?**  
Scales to any data volume without code changes. Deloitte uses Spark for all large-scale ETL.

**Why Parquet + partition by year/month?**  
Query engines skip irrelevant partitions entirely — a date-filtered query on 1M rows reads only 1/24 of the data.

**Why dbt for transformations?**  
SQL transformations are version-controlled, tested, documented, and lineage-tracked automatically. dbt is the Deloitte standard for Snowflake-based warehouses.

**Why Great Expectations before Snowflake load?**  
Bad data in the warehouse is worse than no data — it silently corrupts all downstream reports. GE acts as the data contract enforcement layer.

---

## Author

**Zam** · Data Engineering Portfolio  
Built to demonstrate Deloitte-standard data engineering practices.
