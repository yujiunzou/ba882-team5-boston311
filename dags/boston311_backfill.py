"""
Boston 311 backfill (manual trigger only)

Loads the full CSV files into the raw tables once:
    legacy 2024, 2025, 2026  -> raw_legacy_311
    new system               -> raw_new_311

Steps for each file: create table -> download CSV -> save to GCS -> load to BigQuery.
Rerunning a file replaces that file's rows.

After this, the weekly boston311_incremental DAG keeps the data current.
"""

import csv
import json
import os
import re
import tempfile
from datetime import timedelta
from pathlib import Path

import pendulum
import requests
from airflow.sdk import dag, task
from google.cloud import bigquery, storage

# ==================================================
# 1. Configuration (set in .env)
# ==================================================

GCP_PROJECT = os.environ["GCP_PROJECT"]
GCS_BUCKET = os.environ["GCS_BUCKET"]
BQ_DATASET = os.environ.get("BQ_DATASET", "boston311")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "us-central1")
# GOOGLE_APPLICATION_CREDENTIALS in .env points to the key file.

RESOURCE_SHOW_URL = "https://data.boston.gov/api/3/action/resource_show"
LOCAL_TZ = "America/New_York"

csv.field_size_limit(10_000_000)  # some text fields are long

# Raw tables keep every column as STRING.
LEGACY_COLUMNS = [
    "case_enquiry_id", "open_dt", "sla_target_dt", "closed_dt", "on_time",
    "case_status", "closure_reason", "case_title", "subject", "reason", "type",
    "queue", "department", "submitted_photo", "closed_photo", "location",
    "fire_district", "pwd_district", "city_council_district", "police_district",
    "neighborhood", "neighborhood_services_district", "ward", "precinct",
    "location_street_name", "location_zipcode", "latitude", "longitude",
    "geom_4326", "source",
]

NEW_COLUMNS = [
    "case_id", "open_date", "case_topic", "service_name", "assigned_department",
    "assigned_team", "case_status", "closure_reason", "closure_comments",
    "close_date", "target_close_date", "on_time", "report_source",
    "full_address", "street_number", "street_name", "zip_code", "neighborhood",
    "public_works_district", "city_council_district", "fire_district",
    "police_district", "ward", "precinct", "submitted_photo", "closed_photo",
    "longitude", "latitude",
]

# _loaded_at: when the row was loaded (set by BigQuery)
# _source:    where the row came from, e.g. "backfill_legacy_2024"
METADATA_DDL = """
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _source    STRING
"""

SYSTEMS = {
    "legacy": {"table": "raw_legacy_311", "id": "case_enquiry_id", "columns": LEGACY_COLUMNS},
    "new": {"table": "raw_new_311", "id": "case_id", "columns": NEW_COLUMNS},
}

SOURCES = {
    "legacy_2024": {"system": "legacy", "resource_id": "dff4d804-5031-443a-8409-8344efd0e5c8"},
    "legacy_2025": {"system": "legacy", "resource_id": "9d7c2214-4709-478a-a2e8-fb2020a5bb94"},
    "legacy_2026": {"system": "legacy", "resource_id": "1a0b420d-99f1-4887-9851-990b2a5a6e17"},
    "new_system": {"system": "new", "resource_id": "254adca6-64ab-4c5c-9fc0-a6da622be185"},
}


# ==================================================
# 2. Helpers
# ==================================================

def table_id(name):
    return f"{GCP_PROJECT}.{BQ_DATASET}.{name}"


def normalize(name):
    """Clean a column name, e.g. 'Case Status' -> 'case_status'."""
    name = re.sub(r"[^A-Za-z0-9_]", "_", name.strip()).strip("_").lower()
    if not name or name[0].isdigit():
        name = "col_" + name
    return name


def load_schema(system):
    """Columns in the JSON file (_loaded_at is added by BigQuery)."""
    fields = [bigquery.SchemaField(c, "STRING") for c in SYSTEMS[system]["columns"]]
    return fields + [bigquery.SchemaField("_source", "STRING")]


# ==================================================
# 3. DAG
# ==================================================

@dag(
    dag_id="boston311_backfill",
    start_date=pendulum.datetime(2026, 10, 1, tz=LOCAL_TZ),
    schedule=None,  # manual trigger only
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,  # two files at a time
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["boston311", "backfill"],
)
def boston311_backfill():

    @task
    def create_raw_table(system):
        """Create the raw table if it doesn't exist."""
        cfg = SYSTEMS[system]
        tid = table_id(cfg["table"])
        columns_ddl = ",\n".join(f"    {c} STRING" for c in cfg["columns"])

        client = bigquery.Client(project=GCP_PROJECT)
        client.query(f"""
            CREATE TABLE IF NOT EXISTS `{tid}` (
            {columns_ddl},
            {METADATA_DDL}
            )
        """, location=BQ_LOCATION).result()

        # Stop if an old table without the metadata columns is still there.
        existing = {f.name for f in client.get_table(tid).schema}
        missing = {"_loaded_at", "_source"} - existing
        if missing:
            raise ValueError(
                f"{tid} has no {sorted(missing)} columns. It is the old full-load "
                "table: back it up and drop it, then rerun."
            )
        print(f"{tid} ready")

    @task
    def extract_to_gcs(source):
        cfg = SOURCES[source]
        system = cfg["system"]
        expected = SYSTEMS[system]["columns"]
        id_col = SYSTEMS[system]["id"]

        run_date = pendulum.now(LOCAL_TZ).format("YYYY-MM-DD")

        # Step 1: get the CSV download link
        response = requests.get(RESOURCE_SHOW_URL, params={"id": cfg["resource_id"]}, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if not payload.get("success"):
            raise ValueError(f"{source}: CKAN resource_show failed")
        csv_url = payload["result"]["url"]

        bucket = storage.Client(project=GCP_PROJECT).bucket(GCS_BUCKET)
        prefix = f"boston311/raw/{system}/date={run_date}"

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / f"{source}.csv"
            json_path = Path(temp_dir) / f"{source}.json"

            # Step 2: download the CSV
            with requests.get(csv_url, stream=True, timeout=(30, 300)) as r:
                r.raise_for_status()
                with csv_path.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

            # Step 3: convert to JSON and check each row
            rows = 0
            seen_ids = set()
            with csv_path.open("r", encoding="utf-8-sig", newline="") as f_in, \
                    json_path.open("w", encoding="utf-8") as f_out:
                reader = csv.reader(f_in)
                header = [normalize(h) for h in next(reader, [])]

                if set(header) != set(expected) or len(header) != len(expected):
                    raise ValueError(
                        f"{source}: columns differ from expected. "
                        f"missing={sorted(set(expected) - set(header))} "
                        f"extra={sorted(set(header) - set(expected))}"
                    )

                for line_no, values in enumerate(reader, start=2):
                    if len(values) != len(header):
                        raise ValueError(
                            f"{source}: line {line_no} has {len(values)} fields, expected {len(header)}"
                        )
                    record = {k: (v if v != "" else None) for k, v in zip(header, values)}

                    case_id = record.get(id_col)
                    if not case_id:
                        raise ValueError(f"{source}: line {line_no} has no {id_col}")
                    if case_id in seen_ids:
                        raise ValueError(f"{source}: duplicate {id_col} {case_id}")
                    seen_ids.add(case_id)

                    record["_source"] = f"backfill_{source}"
                    f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
                    rows += 1

            if rows == 0:
                raise ValueError(f"{source}: CSV has no data rows")

            # Step 4: save the CSV and JSON to GCS
            bucket.blob(f"{prefix}/{source}.csv").upload_from_filename(
                str(csv_path), content_type="text/csv", timeout=600
            )
            bucket.blob(f"{prefix}/{source}.json").upload_from_filename(
                str(json_path), content_type="application/json", timeout=600
            )

        gcs_uri = f"gs://{GCS_BUCKET}/{prefix}/{source}.json"
        print(f"{source}: {rows} rows -> {gcs_uri}")
        return {"source": source, "system": system, "gcs_uri": gcs_uri, "rows": rows}

    @task
    def load_to_raw(extracted):
        source = extracted["source"]
        system = extracted["system"]
        client = bigquery.Client(project=GCP_PROJECT)
        target = table_id(SYSTEMS[system]["table"])
        temp = table_id(f"_backfill_{source}")
        schema = load_schema(system)
        tag = f"backfill_{source}"

        try:
            # Step 1: load the JSON into a temporary table
            job = client.load_table_from_uri(
                extracted["gcs_uri"],
                temp,
                job_config=bigquery.LoadJobConfig(
                    schema=schema,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    ignore_unknown_values=False,
                    max_bad_records=0,
                ),
                location=BQ_LOCATION,
            )
            job.result(timeout=900)

            loaded = client.get_table(temp).num_rows
            if loaded != extracted["rows"]:
                raise ValueError(f"{source}: loaded {loaded} rows, expected {extracted['rows']}")

            # Step 2: replace this file's rows (all or nothing)
            cols = ", ".join(f"`{f.name}`" for f in schema)
            client.query(f"""
                BEGIN TRANSACTION;
                DELETE FROM `{target}` WHERE _source = '{tag}';
                INSERT INTO `{target}` ({cols}) SELECT {cols} FROM `{temp}`;
                COMMIT TRANSACTION;
            """, location=BQ_LOCATION).result()

            # Step 3: check the row count
            count_sql = f"SELECT COUNT(*) AS n FROM `{target}` WHERE _source = '{tag}'"
            final = list(client.query(count_sql, location=BQ_LOCATION).result())[0].n
            if final != extracted["rows"]:
                raise ValueError(f"{source}: {final} rows in {target}, expected {extracted['rows']}")

            print(f"{source}: {final} rows in {target}")
            return {"source": source, "table": target, "rows": final}
        finally:
            client.delete_table(temp, not_found_ok=True)

    # ==================================================
    # 4. Task dependencies
    # ==================================================

    ready = {
        system: create_raw_table.override(task_id=f"create_{system}_raw_table")(system)
        for system in SYSTEMS
    }

    for source, cfg in SOURCES.items():
        extracted = extract_to_gcs.override(task_id=f"extract_{source}")(source)
        load_to_raw.override(task_id=f"load_{source}")(extracted)
        ready[cfg["system"]] >> extracted


boston311_backfill()
