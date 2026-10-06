# Retail Sales Intelligence Pipeline

> End-to-end data engineering portfolio project built on the Deloitte enterprise stack.  
> 1M+ raw rows · PySpark · AWS S3 · Snowflake · dbt · Apache Airflow · Great Expectations · Docker

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
| Object Storage    | AWS S3                     | —        |
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
│       ├── retail_ingestion_dag.py   # DAG 1: ingest → S3 → Spark → GE → Snowflake
│       └── retail_transform_dag.py   # DAG 2: dbt staging → dims → facts
│
├── tests/
│   ├── test_data_quality.py      # PySpark/S3 data quality gate (16 checks)
│   └── test_spark_transforms.py  # pytest unit tests for PySpark logic
│
├── config/
│   └── snowflake_setup.sql       # Full Snowflake DDL + permissions
│
├── .github/
│   └── workflows/
│       └── ci.yml                # CI: lint → Spark tests → dbt compile
│
├── docker-compose.yml            # Airflow + Postgres + Metabase
├── requirements.txt
├── .env.example
└── README.md
```

---

## Quick Start (Local Dev)

### Prerequisites
- Python 3.11+
- Docker Desktop
- Java 17 (used by the Airflow/Spark Docker image)
- Snowflake account with the required database, schemas, warehouse, stage, and RAW table

### 1. Clone & setup

```bash
git clone https://github.com/zamwalton/retail-pipeline.git
cd retail-pipeline

python -m venv venv
# Windows PowerShell
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

Copy-Item .env.example .env
# Fill in AWS and Snowflake credentials in .env
```

### 2. Start infrastructure

```bash
docker compose up -d
# Services:
#   Airflow UI  → http://localhost:8080  (admin/admin)
#   Metabase    → http://localhost:3000
#   AWS S3       → real AWS S3 bucket
```

### 3. Run Day 1 pipeline

```bash
# Generate ~1M synthetic orders
python ingestion/generate_data.py

# Upload to S3
python ingestion/upload_to_s3.py

# Spark ETL
python spark_jobs/etl_orders.py

# Data quality gate (runs against S3 staging Parquet)
python tests/test_data_quality.py
```

### 4. Run Snowflake + dbt

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
pytest tests/test_spark_transforms.py -v
```

---

## Pipeline Execution Flow

The production pipeline is split into two Airflow DAGs:

```text
retail_ingestion_dag
    │
    ├── generate_synthetic_data
    ├── upload_to_s3
    ├── validate_raw_s3
    ├── spark_etl_orders
    ├── great_expectations_quality_gate
    ├── log_s3_metrics
    ├── load_snowflake_raw
    └── trigger_transform_dag
                 │
                 ▼
        retail_transform_dag
                 │
                 ├── dbt_install_packages
                 ├── dbt_source_freshness
                 ├── dbt_build_staging
                 ├── dbt_test_staging
                 ├── dbt_build_dimensions
                 ├── dbt_build_fact_sales
                 ├── dbt_test_marts
                 ├── generate_dbt_docs
                 └── notify_pipeline_success
```

### Current validated run

- Raw input: **1,009,924 rows**
- Spark staging output: **985,516 clean rows**
- Data quality gate: **16/16 checks passed**
- Snowflake RAW load: **985,516 rows / 985,516 unique orders**
- `retail_ingestion_dag`: **SUCCESS**
- `retail_transform_dag`: **SUCCESS**
- Transform DAG is automatically triggered after the ingestion DAG completes successfully.

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
The pipeline processes nearly 1M clean staging rows and uses Spark for distributed, S3-based transformation instead of relying on local-memory Pandas processing.

**Why Parquet + partition by year/month?**  
Columnar Parquet storage reduces scan cost, while year/month partitioning lets downstream engines prune irrelevant date partitions.

**Why dbt for transformations?**  
SQL transformations are version-controlled, tested, and organized into staging, dimensions, and fact models for the Snowflake warehouse.

**Why data quality before Snowflake load?**  
The quality gate validates the Spark staging dataset before the warehouse load, blocking the pipeline when critical schema, uniqueness, range, or null checks fail.

---

## Author

**Zam** · Data Engineering Portfolio  
Built to demonstrate Deloitte-standard data engineering practices.
