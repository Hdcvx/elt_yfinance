FROM apache/airflow:3.2.1-python3.11

RUN pip install --no-cache-dir \
    "yfinance>=1.3.0" \
    "loguru>=0.7.3" \
    "psycopg2-binary>=2.9.12" \
    "dbt-core>=1.11.8" \
    "dbt-postgres>=1.10.0"
