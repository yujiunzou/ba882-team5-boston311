
import io
import json
from pathlib import Path

import requests
import pandas as pd

API_URL = "https://data.boston.gov/api/3/action/resource_show"

RESOURCES = {
    "legacy": "1a0b420d-99f1-4887-9851-990b2a5a6e17",
    "new": "254adca6-64ab-4c5c-9fc0-a6da622be185",
}

# Store summary results for both systems
summary = {}

for system, resource_id in RESOURCES.items():
    print(f"\n========== {system.upper()} ==========")

    try:
        # 1. Get CSV URL from CKAN API
        response = requests.get(
            API_URL,
            params={"id": resource_id},
            timeout=30,
        )
        response.raise_for_status()

        payload = response.json()

        if not payload.get("success"):
            raise ValueError("CKAN API returned success=false")

        csv_url = payload["result"]["url"]

        # 2. Download CSV
        csv_response = requests.get(csv_url, timeout=120)
        csv_response.raise_for_status()

        # 3. Read CSV
        df = pd.read_csv(
            io.BytesIO(csv_response.content),
            dtype=str,
            low_memory=False,
        )

        # 4. Inspect data
        print("Rows:", len(df))
        print("Columns:", len(df.columns))

        print("\nColumn names:")
        print(df.columns.tolist())

        print("\nFirst 5 rows:")
        print(df.head().to_string(index=False))

        # 5. Store summary
        summary[system] = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": df.columns.tolist(),
        }

    except (
        requests.RequestException,
        ValueError,
        KeyError,
        pd.errors.ParserError,
    ) as error:
        print("FAILED:", error)

# 6. Save summary as JSON
output_dir = Path("reports")
output_dir.mkdir(parents=True, exist_ok=True)

output_path = output_dir / "311_schema_summary.json"

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2, ensure_ascii=False)

print(f"\nSummary saved to: {output_path}")
