"""
Boston 311 incremental load (weekly)

Every Monday, for each system (legacy, new):
    1. create table   -> make sure the raw table exists
    2. extract new    -> cases opened in the past week (plus 3 extra days, because
                         the city publishes some cases late)
    3. extract open   -> cases still open in our raw table, fetched again so
                         status changes are picked up
    4. load           -> append both files to the raw table

Rows are only appended. dbt keeps the latest version of each case.
Run boston311_backfill once before turning this DAG on.
"""

import json
import os
import time
from datetime import timedelta

import pendulum
import requests
from airflow.sdk import dag, task
from google.cloud import bigquery, storage
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==================================================
# 1. Configuration (set in .env)
# ==================================================

GCP_PROJECT = os.environ["GCP_PROJECT"]
GCS_BUCKET = os.environ["GCS_BUCKET"]
BQ_DATASET = os.environ.get("BQ_DATASET", "boston311")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "us-central1")
# GOOGLE_APPLICATION_CREDENTIALS in .env points to the key file.

SQL_API_URL = "https://data.boston.gov/api/3/action/datastore_search_sql"
LOCAL_TZ = "America/New_York"  # both systems store Boston local time

WINDOW_DAYS = 7      # one week per run
LOOKBACK_DAYS = 3    # extra days to catch late-published cases
PAGE_SIZE = 1000     # rows per API request for new cases
ID_BATCH_SIZE = 200  # case IDs per API request for open cases
REQUEST_DELAY = 0.2  # seconds between API requests

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
# _source:    where the row came from: "api_new" or "api_open" in this DAG
METADATA_DDL = """
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _source    STRING
"""

SYSTEMS = {
    "legacy": {
        # The city starts a new legacy file each year; update this ID in January.
        "resource_id": "1a0b420d-99f1-4887-9851-990b2a5a6e17",  # 2026 file
        "table": "raw_legacy_311",
        "id": "case_enquiry_id",
        "date": "open_dt",
        "columns": LEGACY_COLUMNS,
        # Open cases can only be re-fetched from the current file,
        # so older years are left out of the open-case refresh.
        "refresh_sources": ["backfill_legacy_2026", "api_new", "api_open"],
    },
    "new": {
        "resource_id": "254adca6-64ab-4c5c-9fc0-a6da622be185",
        "table": "raw_new_311",
        "id": "case_id",
        "date": "open_date",
        "columns": NEW_COLUMNS,
        "refresh_sources": ["backfill_new_system", "api_new", "api_open"],
    },
}


# ==================================================
# 2. Helpers
# ==================================================

def table_id(name):
    return f"{GCP_PROJECT}.{BQ_DATASET}.{name}"


def api_session():
    """Requests session that retries on temporary API errors."""
    session = requests.Session()
    retry = Retry(total=4, backoff_factor=1,
                  status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def run_sql(session, sql):
    """Run one SQL query against the city's data API and return the rows."""
    response = session.get(SQL_API_URL, params={"sql": sql}, timeout=90)
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success"):
        raise RuntimeError(f"City API query failed: {payload.get('error')}")
    return payload["result"]["records"]


def clean(rows, columns, source):
    """Keep only the expected columns, as strings, and tag each row."""
    cleaned = []
    for row in rows:
        record = {c: (None if row.get(c) in (None, "") else str(row.get(c))) for c in columns}
        record["_source"] = source
        cleaned.append(record)
    return cleaned


def save_to_gcs(system, run_date, name, rows):
    """Write rows as newline-delimited JSON to GCS. Returns the gs:// path, or None if empty."""
    if not rows:
        print(f"{system} {name}: no rows, nothing to save")
        return None
    path = f"boston311/raw/{system}/date={run_date}/{name}.json"
    data = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
    storage.Client(project=GCP_PROJECT).bucket(GCS_BUCKET).blob(path).upload_from_string(
        data, content_type="application/json"
    )
    uri = f"gs://{GCS_BUCKET}/{path}"
    print(f"{system} {name}: {len(rows)} rows -> {uri}")
    return uri


# ==================================================
# 3. DAG
# ==================================================

@dag(
    dag_id="boston311_incremental",
    schedule="0 12 * * 1",  # Mondays 12:00 Boston time
    start_date=pendulum.datetime(2026, 10, 19, 12, tz=LOCAL_TZ),
    catchup=False,  # history comes from boston311_backfill
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(seconds=30)},
    tags=["boston311", "incremental"],
)
def boston311_incremental():

    @task
    def create_raw_table(system):
        """Create the raw table if it doesn't exist."""
        cfg = SYSTEMS[system]
        columns_ddl = ",\n".join(f"    {c} STRING" for c in cfg["columns"])
        bigquery.Client(project=GCP_PROJECT).query(f"""
            CREATE TABLE IF NOT EXISTS `{table_id(cfg['table'])}` (
            {columns_ddl},
            {METADATA_DDL}
            )
        """, location=BQ_LOCATION).result()
        print(f"{table_id(cfg['table'])} ready")

    @task
    def extract_new_cases(system, data_interval_end=None):
        """Cases opened in the past week plus the lookback days."""
        cfg = SYSTEMS[system]

        # Window in Boston local time, because that's how both systems store dates
        end = data_interval_end.in_timezone(LOCAL_TZ)
        start = end.subtract(days=WINDOW_DAYS + LOOKBACK_DAYS)
        start_str = start.format("YYYY-MM-DD HH:mm:ss")
        end_str = end.format("YYYY-MM-DD HH:mm:ss")
        run_date = end.format("YYYY-MM-DD")
        print(f"{system}: cases opened {start_str} to {end_str}")

        rows = []
        with api_session() as session:
            offset = 0
            while True:
                page = run_sql(session, (
                    f'SELECT * FROM "{cfg["resource_id"]}" '
                    f'WHERE "{cfg["date"]}" >= \'{start_str}\' AND "{cfg["date"]}" < \'{end_str}\' '
                    f'ORDER BY "{cfg["id"]}" LIMIT {PAGE_SIZE} OFFSET {offset}'
                ))
                rows.extend(page)
                if len(page) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE
                time.sleep(REQUEST_DELAY)

        return save_to_gcs(system, run_date, "api_new", clean(rows, cfg["columns"], "api_new"))

    @task
    def extract_open_cases(system, data_interval_end=None):
        """Every case whose latest version in the raw table is not closed."""
        cfg = SYSTEMS[system]
        run_date = data_interval_end.in_timezone(LOCAL_TZ).format("YYYY-MM-DD")
        sources = ", ".join(f"'{s}'" for s in cfg["refresh_sources"])

        # Step 1: find open case IDs (latest version of each case only)
        query = f"""
            SELECT case_id FROM (
                SELECT {cfg['id']} AS case_id, case_status,
                       ROW_NUMBER() OVER (PARTITION BY {cfg['id']} ORDER BY _loaded_at DESC) AS rn
                FROM `{table_id(cfg['table'])}`
                WHERE _source IN ({sources})
            )
            WHERE rn = 1 AND LOWER(TRIM(IFNULL(case_status, ''))) != 'closed'
        """
        client = bigquery.Client(project=GCP_PROJECT)
        ids = [r.case_id for r in client.query(query, location=BQ_LOCATION).result()]
        print(f"{system}: {len(ids)} open cases to re-check")

        # Step 2: fetch their current version from the API, in batches
        rows = []
        missing = 0
        with api_session() as session:
            for i in range(0, len(ids), ID_BATCH_SIZE):
                batch = ids[i:i + ID_BATCH_SIZE]
                id_list = ", ".join(f"'{x}'" for x in batch)
                page = run_sql(session, (
                    f'SELECT * FROM "{cfg["resource_id"]}" WHERE "{cfg["id"]}" IN ({id_list})'
                ))
                missing += len(batch) - len(page)
                rows.extend(page)
                time.sleep(REQUEST_DELAY)

        # A case can disappear from the city's file (e.g. moved to the new system).
        # Log it and keep going instead of failing the whole run.
        if missing:
            print(f"{system}: {missing} open cases were not returned by the API")

        return save_to_gcs(system, run_date, "api_open", clean(rows, cfg["columns"], "api_open"))

    @task
    def load_to_raw(system, gcs_uri):
        """Append a GCS file to the raw table."""
        if gcs_uri is None:
            print("no file to load, skipping")
            return

        cfg = SYSTEMS[system]
        schema = [bigquery.SchemaField(c, "STRING") for c in cfg["columns"]]
        schema.append(bigquery.SchemaField("_source", "STRING"))

        job = bigquery.Client(project=GCP_PROJECT).load_table_from_uri(
            gcs_uri,
            table_id(cfg["table"]),
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                ignore_unknown_values=False,
            ),
            location=BQ_LOCATION,
        )
        job.result()
        print(f"loaded {job.output_rows} rows into {table_id(cfg['table'])}")

    # ==================================================
    # 4. Task dependencies
    # ==================================================

    for system in SYSTEMS:
        ready = create_raw_table.override(task_id=f"create_{system}_raw_table")(system)

        new_file = extract_new_cases.override(task_id=f"extract_{system}_new_cases")(system)
        open_file = extract_open_cases.override(task_id=f"extract_{system}_open_cases")(system)
        ready >> [new_file, open_file]

        load_to_raw.override(task_id=f"load_{system}_new_cases")(system, new_file)
        load_to_raw.override(task_id=f"load_{system}_open_cases")(system, open_file)


boston311_incremental()
