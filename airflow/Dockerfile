
FROM apache/airflow:2.9.1-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow


RUN pip install \
    --no-cache-dir \
    --default-timeout=300 \
    --retries=10 \
    "boto3==1.34.69" \
    "python-dotenv==1.0.1" \
    "loguru==0.7.2" \
    "pandas==2.1.4" \
    "Faker==24.4.0" \
    "pyspark==3.5.1" \
    "dbt-core==1.8.0" \
    "dbt-snowflake==1.8.0" \
    "great-expectations==0.18.13"

