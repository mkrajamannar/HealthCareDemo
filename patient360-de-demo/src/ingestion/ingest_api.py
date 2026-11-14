"""API ingestion: FHIR R4 MedicationRequest bundle -> Bronze (raw JSON).

Demo run (local):  python src/ingestion/ingest_api.py
                   Uses the captured bundle in data/api/ so it works offline.
Live run:          set FHIR_BASE_URL to a FHIR server (e.g. the public HAPI
                   FHIR test server) and the script pages through
                   /MedicationRequest with _count + next-link paging.
"""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

PROJECT = Path(__file__).resolve().parents[2]
API_DIR = PROJECT / "data" / "api"
BRONZE = PROJECT / "data" / "bronze"

FHIR_BASE_URL = os.environ.get("FHIR_BASE_URL", "")  # e.g. https://hapi.fhir.org/baseR4
PAGE_SIZE = 100
MAX_RETRIES = 3


def fetch_live_bundle() -> dict:
    """Page through MedicationRequest resources with retry + backoff."""
    entries: list = []
    url = f"{FHIR_BASE_URL}/MedicationRequest?_count={PAGE_SIZE}"
    while url:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                break
            except requests.RequestException as exc:
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(2 ** attempt)
                print(f"retry {attempt} for {url}: {exc}")
        page = resp.json()
        entries.extend(page.get("entry", []))
        url = next((l["url"] for l in page.get("link", [])
                    if l.get("relation") == "next"), None)
    return {"resourceType": "Bundle", "type": "searchset",
            "total": len(entries), "entry": entries}


def load_bundle() -> dict:
    if FHIR_BASE_URL:
        print(f"fetching live FHIR bundle from {FHIR_BASE_URL}")
        return fetch_live_bundle()
    print("no FHIR_BASE_URL set -- using captured bundle in data/api/")
    return json.loads((API_DIR / "fhir_medication_requests.json").read_text())


def ingest_api() -> Path:
    bundle = load_bundle()
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    dest_dir = BRONZE / "medications" / f"batch_id={batch_id}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / "fhir_medication_requests.json"
    dest.write_text(json.dumps(bundle, indent=2))
    print(f"landed {bundle.get('total', len(bundle.get('entry', [])))} "
          f"MedicationRequests -> {dest}")
    return dest


if __name__ == "__main__":
    ingest_api()
