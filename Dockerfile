FROM astrocrpublic.azurecr.io/runtime:3.3-9

RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-bigquery

COPY dbt /usr/local/airflow/dbt
