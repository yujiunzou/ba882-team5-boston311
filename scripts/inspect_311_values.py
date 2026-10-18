
import io
import requests
import pandas as pd

API_URL = "https://data.boston.gov/api/3/action/resource_show"

RESOURCES = {
    "legacy": "1a0b420d-99f1-4887-9851-990b2a5a6e17",
    "new": "254adca6-64ab-4c5c-9fc0-a6da622be185",
}

CHECK_COLUMNS = {
    "legacy": ["case_status", "on_time", "type", "source"],
    "new": ["case_status", "on_time", "service_name", "report_source"],
}

for system, resource_id in RESOURCES.items():
    print(f"\n========== {system.upper()} ==========")

    response = requests.get(
        API_URL,
        params={"id": resource_id},
        timeout=30,
    )
    response.raise_for_status()

    payload = response.json()
    if not payload.get("success"):
        raise ValueError(f"{system}: CKAN API returned success=false")

    csv_url = payload["result"]["url"]
    csv_response = requests.get(csv_url, timeout=120)
    csv_response.raise_for_status()

    df = pd.read_csv(
        io.BytesIO(csv_response.content),
        dtype=str,
        low_memory=False,
    )

    for col in CHECK_COLUMNS[system]:
        print(f"\n--- {col} ---")
        print(df[col].value_counts(dropna=False).head(15))
