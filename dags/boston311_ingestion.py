
"""
Boston 311 Data Ingestion Pipeline

Pipeline:
    Boston 311 CKAN API
        -> Google Cloud Storage (GCS)
        -> BigQuery Raw Tables

Sources:
    - Legacy Boston 311
    - New Boston 311

During development:
    - Manual trigger only
    - Full refresh of BigQuery raw tables
    - Keep source CSV files in GCS by Airflow run ID
"""

import csv
import re
import tempfile

from datetime import timedelta
from pathlib import Path

import pendulum
import requests

from airflow.sdk import dag, task, get_current_context
from google.cloud import storage, bigquery


# ==================================================
# 1. GCP Configuration
# ==================================================

PROJECT_ID = "ba882-team5-project"
BUCKET_NAME = "ba882-team5-boston311-raw"
DATASET_ID = "boston311"
LOCATION = "us-central1"

API_URL = "https://data.boston.gov/api/3/action/resource_show"

RESOURCES = {
    "legacy": "1a0b420d-99f1-4887-9851-990b2a5a6e17",
    "new": "254adca6-64ab-4c5c-9fc0-a6da622be185",
}


# ==================================================
# 2. DAG Configuration
# ==================================================

@dag(
    dag_id="boston311_ingestion",
    start_date=pendulum.datetime(
        2026, 10, 20, tz="America/New_York"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["boston311", "ingestion"],
)
def boston311_ingestion():

    # ==============================================
    # 3. Extract CSV from API and Upload to GCS
    # ==============================================

    @task
    def extract_to_gcs(system: str) -> dict:

        resource_id = RESOURCES[system]

        # Step 1: Retrieve resource information
        response = requests.get(
            API_URL,
            params={"id": resource_id},
            timeout=30,
        )
        response.raise_for_status()

        payload = response.json()

        if not payload.get("success"):
            raise ValueError(
                f"{system}: CKAN API request failed"
            )

        csv_url = payload["result"]["url"]

        # Step 2: Create a unique GCS path per DAG run
        context = get_current_context()

        run_id = re.sub(
            r"[^A-Za-z0-9_-]",
            "_",
            context["run_id"],
        )

        gcs_path = (
            f"raw/{system}/"
            f"{run_id}/{system}_311.csv"
        )

        # Step 3: Download CSV to temporary storage
        with tempfile.TemporaryDirectory() as temp_dir:

            file_path = Path(temp_dir) / f"{system}_311.csv"

            with requests.get(
                csv_url,
                stream=True,
                timeout=(30, 300),
            ) as csv_response:

                csv_response.raise_for_status()

                with file_path.open("wb") as file:
                    for chunk in csv_response.iter_content(
                        chunk_size=1024 * 1024
                    ):
                        if chunk:
                            file.write(chunk)

            file_size = file_path.stat().st_size

            if file_size == 0:
                raise ValueError(
                    f"{system}: Downloaded CSV is empty"
                )

            # Step 4: Read CSV column names
            with file_path.open(
                "r",
                encoding="utf-8-sig",
                newline="",
            ) as file:

                reader = csv.reader(file)
                columns = next(reader, [])

            if not columns:
                raise ValueError(
                    f"{system}: CSV has no column headers"
                )

            # Step 5: Upload CSV to GCS
            gcs_client = storage.Client(
                project=PROJECT_ID
            )

            bucket = gcs_client.bucket(BUCKET_NAME)
            blob = bucket.blob(gcs_path)

            blob.upload_from_filename(
                str(file_path),
                content_type="text/csv",
                timeout=300,
            )

        gcs_uri = f"gs://{BUCKET_NAME}/{gcs_path}"

        print(f"{system}: Upload successful")
        print(f"GCS URI: {gcs_uri}")
        print(f"File size: {file_size} bytes")
        print(f"Columns: {len(columns)}")

        # Return metadata, not the CSV itself.
        return {
            "gcs_uri": gcs_uri,
            "columns": columns,
            "file_size_bytes": file_size,
        }

    # ==============================================
    # 4. Load CSV from GCS into BigQuery
    # ==============================================

    @task
    def load_to_bigquery(system: str, file_info: dict):

        bq_client = bigquery.Client(
            project=PROJECT_ID
        )

        destination = (
            f"{PROJECT_ID}.{DATASET_ID}."
            f"raw_{system}_311"
        )

        # Step 1: Normalize CSV column names
        names = []

        for original in file_info["columns"]:

            name = re.sub(
                r"[^A-Za-z0-9_]",
                "_",
                original.strip(),
            ).strip("_").lower()

            if not name or name[0].isdigit():
                name = "col_" + name

            names.append(name)

        if len(names) != len(set(names)):
            raise ValueError(
                f"{system}: Duplicate column names"
            )

        # Step 2: Define BigQuery schema
        # Raw layer keeps all columns as STRING.
        schema = [
            bigquery.SchemaField(
                name=name,
                field_type="STRING",
                mode="NULLABLE",
            )
            for name in names
        ]

        # Step 3: Configure BigQuery load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            allow_quoted_newlines=True,
            schema=schema,
            autodetect=False,
            encoding="UTF-8",
            max_bad_records=0,
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE
            ),
            create_disposition=(
                bigquery.CreateDisposition.CREATE_IF_NEEDED
            ),
        )

        # Step 4: Load GCS CSV into BigQuery
        job = bq_client.load_table_from_uri(
            file_info["gcs_uri"],
            destination,
            job_config=job_config,
            location=LOCATION,
        )

        # Wait until the load job finishes.
        job.result(timeout=600)

        # Step 5: Verify loaded table
        table = bq_client.get_table(destination)

        if table.num_rows == 0:
            raise ValueError(
                f"{system}: BigQuery table has zero rows"
            )

        print(f"{system}: BigQuery load successful")
        print(f"Table: {destination}")
        print(f"Rows: {table.num_rows}")
        print(f"Columns: {len(table.schema)}")

        return {
            "table": destination,
            "row_count": table.num_rows,
        }

    # ==============================================
    # 5. Define Task Dependencies
    # ==============================================

    # Legacy pipeline
    legacy_file = extract_to_gcs.override(
        task_id="extract_legacy"
    )("legacy")

    load_to_bigquery.override(
        task_id="load_legacy_bigquery"
    )("legacy", legacy_file)

    # New pipeline
    new_file = extract_to_gcs.override(
        task_id="extract_new"
    )("new")

    load_to_bigquery.override(
        task_id="load_new_bigquery"
    )("new", new_file)


# Register DAG with Airflow
boston311_ingestion()
