"""
Agricultural Commodity Price Intelligence — v0.1 ingestion script.

Pulls daily mandi price records from the data.gov.in Agmarknet dataset
for a single commodity + state combination, and stores them in a local
SQLite database for downstream analysis.

This is intentionally the simplest useful version. No orchestration,
no cloud, no dbt — just: API -> parse -> SQLite -> done.
"""

import os
import sqlite3
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

# --- Configuration ----------------------------------------------------------

# data.gov.in endpoint for "Current daily price of various commodities
# from various markets (Mandi)". The UUID in the path identifies this
# specific dataset.
API_ENDPOINT = (
    "https://api.data.gov.in/resource/"
    "9ef84268-d588-465a-a308-a864a43d0070"
)

# Pull target — tweak these to grab different slices.
STATE = "Tamil Nadu"
COMMODITY = "Banana"

# How many records to request in this run.
LIMIT = 1000

# Where the local SQLite database lives.
DB_PATH = "data/mandi_prices.db"


# --- API call ---------------------------------------------------------------

def fetch_price_records(api_key: str) -> list[dict]:
    """Call the Agmarknet endpoint and return a list of price record dicts."""
    params = {
        "api-key": api_key,
        "format": "json",
        "limit": LIMIT,
        "filters[state]": STATE,
        "filters[commodity]": COMMODITY,
    }

    response = requests.get(API_ENDPOINT, params=params, timeout=30)
    response.raise_for_status()  # crash loudly if the API returns an error

    payload = response.json()
    records = payload.get("records", [])
    print(f"API returned {len(records)} records for {COMMODITY} in {STATE}.")
    return records


# --- Storage ----------------------------------------------------------------

def ensure_db(db_path: str) -> sqlite3.Connection:
    """Create the SQLite DB and the table if they don't already exist."""
    # Make sure the data/ directory exists.
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mandi_prices (
            state        TEXT,
            district     TEXT,
            market       TEXT,
            commodity    TEXT,
            variety      TEXT,
            arrival_date TEXT,
            min_price    REAL,
            max_price    REAL,
            modal_price  REAL,
            ingested_at  TEXT,
            PRIMARY KEY (state, district, market, commodity, variety, arrival_date)
        )
        """
    )
    conn.commit()
    return conn


def insert_records(conn: sqlite3.Connection, records: list[dict]) -> int:
    """Insert records into the table. Ignore duplicates on the primary key."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    rows = [
        (
            r.get("state"),
            r.get("district"),
            r.get("market"),
            r.get("commodity"),
            r.get("variety"),
            r.get("arrival_date"),
            _to_float(r.get("min_price")),
            _to_float(r.get("max_price")),
            _to_float(r.get("modal_price")),
            ingested_at,
        )
        for r in records
    ]

    cursor = conn.executemany(
        """
        INSERT OR IGNORE INTO mandi_prices (
            state, district, market, commodity, variety, arrival_date,
            min_price, max_price, modal_price, ingested_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return cursor.rowcount


def _to_float(value) -> float | None:
    """API sometimes returns prices as strings; coerce safely."""
    if value in (None, "", "NA"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# --- Main -------------------------------------------------------------------

def main():
    load_dotenv()
    api_key = os.getenv("DATA_GOV_IN_API_KEY")
    if not api_key:
        raise RuntimeError(
            "DATA_GOV_IN_API_KEY not found. "
            "Check that a .env file exists with your key."
        )

    records = fetch_price_records(api_key)
    if not records:
        print("No records returned. Nothing to insert.")
        return

    conn = ensure_db(DB_PATH)
    try:
        inserted = insert_records(conn, records)
        total = conn.execute(
            "SELECT COUNT(*) FROM mandi_prices"
        ).fetchone()[0]
    finally:
        conn.close()

    print(f"Inserted {inserted} new rows. Total rows in DB: {total}.")


if __name__ == "__main__":
    main()