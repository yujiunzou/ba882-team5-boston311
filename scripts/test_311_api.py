
import requests

API_URL = "https://data.boston.gov/api/3/action/resource_show"

RESOURCES = {
    "legacy": "1a0b420d-99f1-4887-9851-990b2a5a6e17",
    "new": "254adca6-64ab-4c5c-9fc0-a6da622be185",
}

for system, resource_id in RESOURCES.items():
    print(f"\n=== {system.upper()} SYSTEM ===")

    try:
        response = requests.get(
            API_URL,
            params={"id": resource_id},
            timeout=30,
        )
        response.raise_for_status()

        payload = response.json()
        if not payload.get("success"):
            raise ValueError("CKAN API returned success=false")

        resource = payload["result"]

        print("Resource name:", resource.get("name"))
        print("Format:", resource.get("format"))
        print("CSV URL:", resource.get("url"))
        print("API metadata: SUCCESS")

    except (requests.RequestException, ValueError, KeyError) as error:
        print("API test FAILED:", error)
