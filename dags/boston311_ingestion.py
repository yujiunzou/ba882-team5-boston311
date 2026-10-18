
from datetime import datetime
from pathlib import Path

import requests
from airflow.sdk import dag, task

API_URL = "https://data.boston.gov/api/3/action/resource_show"

RESOURCES = {
    "legacy": "1a0b420d-99f1-4887-9851-990b2a5a6e17",
    "new": "254adca6-64ab-4c5c-9fc0-a6da622be185",
}


def download_csv(system: str) -> dict:
    """Download one Boston 311 CSV resource."""

    resource_id = RESOURCES[system]

    # 1. Retrieve CSV URL
    response = requests.get(
        API_URL,
        params={"id": resource_id},
        timeout=30,
    )
    response.raise_for_status()

    payload = response.json()

    if not payload.get("success"):
        raise ValueError(f"{system}: CKAN API failed")

    csv_url = payload["result"]["url"]

    # 2. Download CSV
    csv_response = requests.get(
        csv_url,
        timeout=120,
    )
    csv_response.raise_for_status()

    # 3. Save CSV in local Airflow container
    output_dir = Path("/tmp/boston311")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{system}_311.csv"
    output_path.write_bytes(csv_response.content)

    file_size_mb = output_path.stat().st_size / (1024 * 1024)

    print(f"System: {system}")
    print(f"File: {output_path}")
    print(f"Size: {file_size_mb:.2f} MB")
    print("Download successful")

    return {
        "system": system,
        "file_path": str(output_path),
        "file_size_mb": round(file_size_mb, 2),
    }


@dag(
    dag_id="boston311_ingestion",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["boston311", "ingestion"],
)
def boston311_ingestion():

    @task
    def extract_legacy():
        return download_csv("legacy")

    @task
    def extract_new():
        return download_csv("new")

    extract_legacy()
    extract_new()


boston311_ingestion()
