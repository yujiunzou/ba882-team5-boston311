"""Boston 311 incremental ingestion.

Fetch newly opened cases and rotate through unresolved cases from the current
target tables. Stage and MERGE into the target tables; never mutate raw tables.
Keep this DAG paused until a full DAG run and validation are successful.
"""

import json
import logging
import os
import re
import time
from datetime import timedelta

import pendulum
import requests
from airflow.sdk import dag, get_current_context, task
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOG = logging.getLogger(__name__)
API_URL = "https://data.boston.gov/api/3/action/datastore_search_sql"
BQ_DATASET = "boston311"
BQ_LOCATION = "us-central1"
PAGE_SIZE = 100
BATCH_SIZE = 100
REQUEST_DELAY_SECONDS = 0.2
OPEN_CASE_LIMIT = 1000  # Rotate this many unresolved cases per system per run.
PROGRESS_TABLE = "incremental_refresh_progress"

RESOURCES = {
    "legacy": {
        "resource_id": "1a0b420d-99f1-4887-9851-990b2a5a6e17",
        "id": "case_enquiry_id",
        "date": "open_dt",
        "raw": "raw_legacy_311",
        "staging": "stg_legacy_311_incremental_v2",
        "target": "legacy_311_incremental_sandbox_v2",
    },
    "new": {
        "resource_id": "254adca6-64ab-4c5c-9fc0-a6da622be185",
        "id": "case_id",
        "date": "open_date",
        "raw": "raw_new_311",
        "staging": "stg_new_311_incremental_v2",
        "target": "new_311_incremental_sandbox_v2",
    },
}


def session_with_retry():
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


def escape_sql_string(value):
    return str(value).replace("'", "''")


def fetch_sql(session, sql):
    response = session.get(API_URL, params={"sql": sql}, timeout=90)
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success"):
        raise RuntimeError(f"Boston CKAN query failed: {payload.get('error')}")
    return payload.get("result", {}).get("records", [])


def clean_records(records):
    return [
        {key: (None if value is None else str(value)) for key, value in record.items()
         if key not in ("_id", "_full_text")}
        for record in records
    ]


def save_gcs(project, bucket, system, label, records):
    run_id = get_current_context()["run_id"]
    safe_run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
    path = f"incremental/v3/{system}/{safe_run_id}/{label}.jsonl"
    data = "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records)
    storage.Client(project=project).bucket(bucket).blob(path).upload_from_string(
        data, content_type="application/x-ndjson"
    )
    uri = f"gs://{bucket}/{path}"
    LOG.info("Uploaded %d %s/%s records to %s", len(records), system, label, uri)
    return {"gcs_uri": uri, "count": len(records), "system": system, "label": label}


def progress_table_id(project):
    return f"{project}.{BQ_DATASET}.{PROGRESS_TABLE}"


def read_progress(client, project, system):
    query = f"""
        SELECT last_case_id
        FROM `{progress_table_id(project)}`
        WHERE system = @system
        ORDER BY updated_at DESC
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("system", "STRING", system)
    ])
    rows = list(client.query(query, job_config=job_config, location=BQ_LOCATION).result())
    return rows[0].last_case_id if rows else None


@dag(
    dag_id="boston311_incremental",
    start_date=pendulum.datetime(2026, 10, 21, tz="America/New_York"),
    schedule="0 0 * * *",
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["boston311", "incremental"],
)
def boston311_incremental():
    @task
    def extract_new(system):
        cfg = RESOURCES[system]
        project = os.environ["GCP_PROJECT"]
        bucket = os.environ["GCS_BUCKET"]
        context = get_current_context()
        # Use the completed daily interval, not the run's wall-clock time.
        # Half-open bounds prevent overlap at day boundaries.
        interval_start = context["data_interval_start"]
        interval_end = context["data_interval_end"]
        # Some Airflow 3 timetables expose a zero-length daily interval.
        # Fall back to the preceding New York calendar day in that case.
        if interval_start >= interval_end:
            interval_start = interval_end.in_timezone("America/New_York").subtract(days=1)
        start = interval_start.in_timezone("UTC").format("YYYY-MM-DD HH:mm:ss")
        end = interval_end.in_timezone("UTC").format("YYYY-MM-DD HH:mm:ss")
        records = []
        seen = set()
        with session_with_retry() as session:
            offset = 0
            while True:
                sql = (
                    f'SELECT * FROM "{cfg["resource_id"]}" '
                    f'WHERE "{cfg["date"]}" >= \'{escape_sql_string(start)}\' '
                    f'AND "{cfg["date"]}" < \'{escape_sql_string(end)}\' '
                    f'ORDER BY "{cfg["date"]}" DESC, "{cfg["id"]}" DESC '
                    f'LIMIT {PAGE_SIZE} OFFSET {offset}'
                )
                page = clean_records(fetch_sql(session, sql))
                LOG.info("%s new offset %d: %d", system, offset, len(page))
                for row in page:
                    case_id = row.get(cfg["id"])
                    if not case_id or case_id in seen:
                        raise ValueError(f"{system} new cases missing or duplicate ID: {case_id}")
                    seen.add(case_id)
                records.extend(page)
                if len(page) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE
                time.sleep(REQUEST_DELAY_SECONDS)
        return save_gcs(project, bucket, system, "new", records)

    @task
    def extract_open(system):
        cfg = RESOURCES[system]
        project = os.environ["GCP_PROJECT"]
        bucket = os.environ["GCS_BUCKET"]
        client = bigquery.Client(project=project)
        target_id = f"{project}.{BQ_DATASET}.{cfg['target']}"
        try:
            client.get_table(target_id)
            open_source = target_id
        except NotFound:
            open_source = f"{project}.{BQ_DATASET}.{cfg['raw']}"

        cursor = read_progress(client, project, system)
        if not isinstance(OPEN_CASE_LIMIT, int) or OPEN_CASE_LIMIT < 1:
            raise ValueError("OPEN_CASE_LIMIT must be a positive integer")

        def select_ids(after_id):
            extra = "AND CAST(`{}` AS STRING) > @cursor".format(cfg["id"]) if after_id is not None else ""
            sql = f"""
                SELECT DISTINCT CAST(`{cfg['id']}` AS STRING) AS case_id
                FROM `{open_source}`
                WHERE `{cfg['id']}` IS NOT NULL
                  AND TRIM(CAST(`{cfg['id']}` AS STRING)) != ''
                  AND (case_status IS NULL OR LOWER(TRIM(case_status)) != 'closed')
                  {extra}
                ORDER BY case_id
                LIMIT {OPEN_CASE_LIMIT}
            """
            params = ([bigquery.ScalarQueryParameter("cursor", "STRING", after_id)]
                      if after_id is not None else [])
            config = bigquery.QueryJobConfig(query_parameters=params)
            return [r.case_id for r in client.query(sql, job_config=config, location=BQ_LOCATION).result()]

        ids = select_ids(cursor)
        if not ids and cursor is not None:
            ids = select_ids(None)
        LOG.info("%s unresolved selected=%d previous_cursor=%s", system, len(ids), cursor)
        records = []
        with session_with_retry() as session:
            for offset in range(0, len(ids), BATCH_SIZE):
                batch = ids[offset:offset + BATCH_SIZE]
                values = ", ".join(f"'{escape_sql_string(x)}'" for x in batch)
                sql = (f'SELECT * FROM "{cfg["resource_id"]}" '
                       f'WHERE "{cfg["id"]}" IN ({values}) LIMIT {len(batch) + 1}')
                page = clean_records(fetch_sql(session, sql))
                returned = [row.get(cfg["id"]) for row in page]
                if len(returned) != len(batch) or set(returned) != set(batch):
                    raise ValueError(
                        f"{system} unresolved batch incomplete: "
                        f"missing={sorted(set(batch) - set(returned))[:10]}"
                    )
                records.extend(page)
                time.sleep(REQUEST_DELAY_SECONDS)
        result = save_gcs(project, bucket, system, "open", records)
        result["last_case_id"] = ids[-1] if ids else cursor
        return result

    @task
    def stage_and_merge(system, new_result, open_result):
        cfg = RESOURCES[system]
        project = os.environ["GCP_PROJECT"]
        client = bigquery.Client(project=project)
        raw = f"{project}.{BQ_DATASET}.{cfg['raw']}"
        staging = f"{project}.{BQ_DATASET}.{cfg['staging']}"
        target = f"{project}.{BQ_DATASET}.{cfg['target']}"
        id_col = cfg["id"]
        raw_schema = client.get_table(raw).schema
        columns = [field.name for field in raw_schema]
        if id_col not in columns:
            raise ValueError(f"Missing {id_col} in raw schema")
        # Target table must already exist; do not create a partial table from a daily delta.
        client.get_table(target)
        suffix = re.sub(r"[^A-Za-z0-9_]", "_", get_current_context()["run_id"])[-70:]
        temp_tables = []
        try:
            for result in (new_result, open_result):
                if result["system"] != system:
                    raise ValueError("Unexpected system in task input")
                if result["count"] == 0:
                    continue
                temp = f"{project}.{BQ_DATASET}._inc_{system}_{result['label']}_{suffix}"
                job = client.load_table_from_uri(
                    result["gcs_uri"], temp,
                    job_config=bigquery.LoadJobConfig(
                        schema=raw_schema,
                        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                        ignore_unknown_values=False,
                    ),
                    location=BQ_LOCATION,
                )
                job.result()
                actual = client.get_table(temp).num_rows
                if actual != result["count"]:
                    raise ValueError(f"{system} {result['label']} count mismatch {actual} != {result['count']}")
                temp_tables.append((result["label"], temp))

            if temp_tables:
                union_sql = " UNION ALL ".join(
                    f"SELECT t.*, {2 if label == 'open' else 1} AS _priority FROM `{table}` t"
                    for label, table in temp_tables
                )
                duplicate_sql = f"""
                    WITH input AS ({union_sql})
                    SELECT `{id_col}`, _priority, COUNT(*) n
                    FROM input GROUP BY 1, 2 HAVING COUNT(*) > 1 LIMIT 1
                """
                if list(client.query(duplicate_sql, location=BQ_LOCATION).result()):
                    raise ValueError(f"{system}: duplicate IDs within one source")
                quoted_columns = ", ".join(f"`{col}`" for col in columns)
                client.query(f"""
                    CREATE OR REPLACE TABLE `{staging}` AS
                    WITH input AS ({union_sql})
                    SELECT {quoted_columns}
                    FROM input
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY `{id_col}` ORDER BY _priority DESC
                    ) = 1
                """, location=BQ_LOCATION).result()
                updates = ", ".join(f"T.`{col}` = S.`{col}`" for col in columns if col != id_col)
                insert_values = ", ".join(f"S.`{col}`" for col in columns)
                client.query(f"""
                    MERGE `{target}` T USING `{staging}` S
                    ON T.`{id_col}` = S.`{id_col}`
                    WHEN MATCHED AND TO_JSON_STRING(T) != TO_JSON_STRING(S)
                        THEN UPDATE SET {updates}
                    WHEN NOT MATCHED THEN
                        INSERT ({quoted_columns}) VALUES ({insert_values})
                """, location=BQ_LOCATION).result()

            # Advance the cursor ONLY after all staging and target operations succeeded.
            # If progress writing fails, replaying the batch is safe because MERGE is idempotent.
            next_cursor = open_result.get("last_case_id")
            if open_result["count"] > 0 and next_cursor is not None:
                config = bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("system", "STRING", system),
                    bigquery.ScalarQueryParameter("cursor", "STRING", next_cursor),
                ])
                client.query(f"""
                    MERGE `{progress_table_id(project)}` T
                    USING (SELECT @system AS system, @cursor AS last_case_id) S
                    ON T.system = S.system
                    WHEN MATCHED THEN UPDATE SET
                        last_case_id = S.last_case_id, updated_at = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (system, last_case_id, updated_at)
                        VALUES (S.system, S.last_case_id, CURRENT_TIMESTAMP())
                """, job_config=config, location=BQ_LOCATION).result()
            LOG.info("%s target merge complete; cursor=%s", system, next_cursor)
            return {"system": system, "target": target, "cursor": next_cursor}
        finally:
            for _, temp in temp_tables:
                client.delete_table(temp, not_found_ok=True)

    for system in ("legacy", "new"):
        newly_opened = extract_new.override(task_id=f"extract_{system}_new_cases")(system)
        unresolved = extract_open.override(task_id=f"extract_{system}_open_cases")(system)
        stage_and_merge.override(task_id=f"stage_merge_{system}")(
            system, newly_opened, unresolved
        )


boston311_incremental()
