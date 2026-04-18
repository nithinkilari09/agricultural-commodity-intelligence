"""
One-off exploration script. Calls the API with no filters to see
what's actually in the current dataset today.
"""

import os
import requests
from collections import Counter
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("DATA_GOV_IN_API_KEY")

API_ENDPOINT = (
    "https://api.data.gov.in/resource/"
    "9ef84268-d588-465a-a308-a864a43d0070"
)

params = {
    "api-key": api_key,
    "format": "json",
    "limit": 1000,  # pull a big sample
}

response = requests.get(API_ENDPOINT, params=params, timeout=30)
response.raise_for_status()
payload = response.json()
records = payload.get("records", [])

print(f"Total records returned: {len(records)}")
print(f"Total available today (from API metadata): {payload.get('total', 'unknown')}")
print()

if records:
    # Show one sample record so we see the shape
    print("Sample record:")
    for k, v in records[0].items():
        print(f"  {k}: {v}")
    print()

    # Top 10 states by record count
    states = Counter(r.get("state") for r in records)
    print("Top states in sample:")
    for state, count in states.most_common(10):
        print(f"  {state}: {count}")
    print()

    # Top 10 commodities
    commodities = Counter(r.get("commodity") for r in records)
    print("Top commodities in sample:")
    for commodity, count in commodities.most_common(10):
        print(f"  {commodity}: {count}")
    print()

    # AP-specific commodities
    ap_records = [r for r in records if r.get("state") == "Andhra Pradesh"]
    print(f"Records from Andhra Pradesh in sample: {len(ap_records)}")
    if ap_records:
        ap_commodities = Counter(r.get("commodity") for r in ap_records)
        print("Commodities currently in AP:")
        for c, n in ap_commodities.most_common():
            print(f"  {c}: {n}")