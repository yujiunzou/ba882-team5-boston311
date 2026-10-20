
import json
import re
from datetime import datetime, timezone
from urllib.request import urlopen
from urllib.parse import urlencode

from google.cloud import storage, bigquery

from airflow.sdk import DAG, get_current_context
from airflow.providers.standard.operators.python import PythonOperator


PROJECT_ID = "ba882-team5-project"
BUCKET_NAME = "ba882-team5-boston311-raw"
DATASET_ID = "boston311_andy_dev"
TABLE_ID = "raw_new_311"

RESOURCE_ID = "254adca6-64ab-4c5c-9fc0-a6da622be185"


# Task 1: Boston 311 API → GCS
def extract_new_311():

    url = "https://data.boston.gov/api/3/action/datastore_search"

    params = {
        "resource_id": RESOURCE_ID,
        "limit": 100
    }

    api_url = url + "?" + urlencode(params)

    with urlopen(api_url, timeout=30) as response:
        data = json.load(response)

    if not data.get("success"):
        raise ValueError("Boston 311 API request failed")

    records = data["result"]["records"]

    if not records:
        raise ValueError("No records returned")

    print("Boston 311 API connected!")
    print("Rows:", len(records))

    # Convert data to JSONL
    jsonl_data = "\n".join(
        json.dumps(record, ensure_ascii=False, default=str)
        for record in records
    ) + "\n"

    # Generate unique filename
    context = get_current_context()

    run_id = re.sub(
        r"[^A-Za-z0-9_-]", "_", context["run_id"]
    )

    file_name = f"raw_new_311_{run_id}.jsonl"

    gcs_path = f"experiments/andy/new311/{file_name}"

    # Upload to GCS
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)

    blob.upload_from_string(
        jsonl_data,
        content_type="application/x-ndjson"
    )

    gcs_uri = f"gs://{BUCKET_NAME}/{gcs_path}"

    print("GCS Upload successful!")
    print("GCS Path:", gcs_uri)

    return gcs_uri


# Task 2: GCS → BigQuery
def load_to_bigquery():

    context = get_current_context()

    # Get file path from Task 1
    gcs_uri = context["ti"].xcom_pull(
        task_ids="extract_new_311"
    )

    if not gcs_uri:
        raise ValueError("No GCS file path received")

    client = bigquery.Client(project=PROJECT_ID)

    destination = (
        f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    )

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    # Load GCS file into BigQuery
    job = client.load_table_from_uri(
        gcs_uri,
        destination,
        job_config=job_config,
        location="us-central1"
    )

    job.result()

    table = client.get_table(destination)

    print("BigQuery Load successful!")
    print("Destination:", destination)
    print("Total Rows:", table.num_rows)


# Define Airflow DAG
with DAG(
    dag_id="boston_311_api_test",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["boston311"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_new_311",
        python_callable=extract_new_311,
    )

    load_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
    )

    extract_task >> load_task
