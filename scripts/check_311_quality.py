
import io
import json
from pathlib import Path

import pandas as pd
import requests

API_URL = "https://data.boston.gov/api/3/action/resource_show"

RESOURCES = {
    "legacy": {
        "id": "1a0b420d-99f1-4887-9851-990b2a5a6e17",
        "case_id": "case_enquiry_id",
        "opened_at": "open_dt",
        "closed_at": "closed_dt",
        "sla_due_at": "sla_target_dt",
    },
    "new": {
        "id": "254adca6-64ab-4c5c-9fc0-a6da622be185",
        "case_id": "case_id",
        "opened_at": "open_date",
        "closed_at": "close_date",
        "sla_due_at": "target_close_date",
    },
}

results = {}
case_ids = {}

for system, config in RESOURCES.items():
    print(f"\n========== {system.upper()} ==========")

    # Get CSV URL
    response = requests.get(
        API_URL,
        params={"id": config["id"]},
        timeout=30,
    )
    response.raise_for_status()

    payload = response.json()
    if not payload.get("success"):
        raise ValueError(f"{system}: CKAN API failed")

    csv_url = payload["result"]["url"]

    # Download CSV
    csv_response = requests.get(csv_url, timeout=120)
    csv_response.raise_for_status()

    df = pd.read_csv(
        io.BytesIO(csv_response.content),
        dtype=str,
        low_memory=False,
    )

    # Check case IDs
    id_col = config["case_id"]
    ids = df[id_col].astype("string").str.strip()
    ids = ids.replace("", pd.NA)

    missing_ids = int(ids.isna().sum())
    duplicate_ids = int(ids.dropna().duplicated().sum())

    case_ids[system] = set(ids.dropna())

    # Check date columns
    date_results = {}

    for field in ["opened_at", "closed_at", "sla_due_at"]:
        col = config[field]

        values = df[col].astype("string").str.strip()
        values = values.replace("", pd.NA)

        parsed = pd.to_datetime(
            values,
            format="mixed",
            errors="coerce",
            utc=True,
        )

        date_results[field] = {
            "source_column": col,
            "missing": int(values.isna().sum()),
            "invalid": int(
                (values.notna() & parsed.isna()).sum()
            ),
            "sample_values": values.dropna().head(3).tolist(),
        }

    results[system] = {
        "row_count": len(df),
        "missing_case_ids": missing_ids,
        "duplicate_case_ids": duplicate_ids,
        "dates": date_results,
    }

    print("Rows:", len(df))
    print("Missing Case IDs:", missing_ids)
    print("Duplicate Case IDs:", duplicate_ids)

    for field, stats in date_results.items():
        print(
            f"{field}: missing={stats['missing']}, "
            f"invalid={stats['invalid']}"
        )

# Check overlapping IDs between systems
overlap = case_ids["legacy"] & case_ids["new"]

results["cross_system"] = {
    "overlapping_case_ids": len(overlap),
    "sample_overlapping_ids": sorted(overlap)[:10],
}

print("\n========== CROSS-SYSTEM ==========")
print("Overlapping Case IDs:", len(overlap))

# Save quality report
output_dir = Path("reports")
output_dir.mkdir(exist_ok=True)

output_path = output_dir / "311_quality_report.json"

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

print(f"\nReport saved to: {output_path}")
