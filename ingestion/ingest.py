"""
Agricultural Commodity Price Intelligence — v0.2 ingestion script.

Pulls daily mandi price records from the data.gov.in Agmarknet API
for a configured set of state + commodity combinations, and loads
them into a Postgres warehouse under the `raw` schema.

Upserts are idempotent: running the script repeatedly against the
same day's data will not create duplicates. Meant to be invoked
on a schedule (locally or from GitHub Actions).
"""

import os
import sys
import time
from datetime import datetime, timezone
from typing import Iterable

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# --- Configuration ----------------------------------------------------------

API_ENDPOINT = (
    "https://api.data.gov.in/resource/"
    "9ef84268-d588-465a-a308-a864a43d0070"
)

# State + commodity pairs to pull every run. Keep this short for now —
# we'll grow it in later versions once we see what's actually available.
TARGETS: list[tuple[str | None, str | None]] = [
    ("Tamil Nadu", "Banana"),
    ("Tamil Nadu", "Papaya"),
    ("Andhra Pradesh", None),   # None = all commodities in that state
    ("Karnataka", None),
    ("Maharashtra", "Onion"),
]

# How many records per API call (max the endpoint tends to allow).
LIMIT_PER_CALL = 1000

# Postgres schema to write raw ingested data into.
RAW_SCHEMA = "raw"
RAW_TABLE = "mandi_prices"


# --- DB helpers -------------------------------------------------------------

def get_engine() -> Engine:
    """Build a SQLAlchemy engine from DATABASE_URL in .env."""
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not found in .env")

    # SQLAlchemy needs to know which Postgres driver to use.
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)

    return create_engine(db_url, pool_pre_ping=True)


def ensure_schema(engine: Engine) -> None:
    """Create the raw schema and table if they don't exist."""
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};

    CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{RAW_TABLE} (
        state         VARCHAR(100),
        district      VARCHAR(100),
        market        VARCHAR(200),
        commodity     VARCHAR(100),
        variety       VARCHAR(100),
        grade         VARCHAR(100),
        arrival_date  VARCHAR(20),
        min_price     NUMERIC(12, 2),
        max_price     NUMERIC(12, 2),
        modal_price   NUMERIC(12, 2),
        ingested_at   TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (state, district, market, commodity, variety, grade, arrival_date)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


# --- API ------------------------------------------------------------------
# Retry behavior for the API call.
MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 2


def fetch(api_key: str, state: str | None, commodity: str | None) -> list[dict]:
    """
    Call the Agmarknet endpoint for one state+commodity slice.

    Retries transient failures (timeouts, connection errors, 5xx responses)
    up to MAX_RETRIES times with exponential backoff. Raises on final failure.
    """
    params: dict[str, str | int] = {
        "api-key": api_key,
        "format": "json",
        "limit": LIMIT_PER_CALL,
    }
    if state:
        params["filters[state]"] = state
    if commodity:
        params["filters[commodity]"] = commodity

    label = f"{commodity or 'ALL'} in {state or 'ALL'}"

    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(API_ENDPOINT, params=params, timeout=60)
            response.raise_for_status()
            records = response.json().get("records", [])
            print(f"  API returned {len(records):>4} records for {label}.")
            return records

        except (requests.Timeout, requests.ConnectionError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                wait = BACKOFF_BASE_SECONDS ** attempt
                print(
                    f"  ⚠  Attempt {attempt}/{MAX_RETRIES} failed for "
                    f"{label} ({type(e).__name__}). Retrying in {wait}s..."
                )
                time.sleep(wait)
            else:
                print(f"  ✗  Final attempt failed for {label}.")

        except requests.HTTPError as e:
            # 4xx errors are not transient — don't retry, fail fast.
            if e.response is not None and 400 <= e.response.status_code < 500:
                print(
                    f"  ✗  Non-retriable HTTP {e.response.status_code} "
                    f"for {label}."
                )
                raise
            # 5xx is transient; retry.
            last_error = e
            if attempt < MAX_RETRIES:
                wait = BACKOFF_BASE_SECONDS ** attempt
                print(
                    f"  ⚠  Attempt {attempt}/{MAX_RETRIES} got HTTP "
                    f"{e.response.status_code if e.response else '?'}. "
                    f"Retrying in {wait}s..."
                )
                time.sleep(wait)

    # All retries exhausted.
    assert last_error is not None
    raise last_error

# --- Loading ----------------------------------------------------------------

def upsert_records(engine: Engine, records: Iterable[dict]) -> int:
    """Insert records with ON CONFLICT DO NOTHING (idempotent)."""
    sql = text(f"""
        INSERT INTO {RAW_SCHEMA}.{RAW_TABLE} (
            state, district, market, commodity, variety, grade,
            arrival_date, min_price, max_price, modal_price, ingested_at
        )
        VALUES (
            :state, :district, :market, :commodity, :variety, :grade,
            :arrival_date, :min_price, :max_price, :modal_price, :ingested_at
        )
        ON CONFLICT (state, district, market, commodity, variety, grade, arrival_date)
        DO NOTHING
    """)

    now = datetime.now(timezone.utc)
    rows = [_row_from_record(r, now) for r in records]
    if not rows:
        return 0

    with engine.begin() as conn:
        result = conn.execute(sql, rows)
    return result.rowcount or 0


def _row_from_record(r: dict, ingested_at: datetime) -> dict:
    return {
        "state": r.get("state"),
        "district": r.get("district"),
        "market": r.get("market"),
        "commodity": r.get("commodity"),
        "variety": r.get("variety") or "",
        "grade": r.get("grade") or "",
        "arrival_date": r.get("arrival_date"),
        "min_price": _to_decimal(r.get("min_price")),
        "max_price": _to_decimal(r.get("max_price")),
        "modal_price": _to_decimal(r.get("modal_price")),
        "ingested_at": ingested_at,
    }


def _to_decimal(value) -> float | None:
    if value in (None, "", "NA"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# --- Main -------------------------------------------------------------------

def main() -> int:
    load_dotenv()
    api_key = os.getenv("DATA_GOV_IN_API_KEY")
    if not api_key:
        raise RuntimeError("DATA_GOV_IN_API_KEY not found in .env")

    engine = get_engine()
    ensure_schema(engine)

    total_fetched = 0
    total_inserted = 0

    failed_targets: list[tuple[str | None, str | None]] = []

    for state, commodity in TARGETS:
        try:
            records = fetch(api_key, state=state, commodity=commodity)
            total_fetched += len(records)
            if records:
                inserted = upsert_records(engine, records)
                total_inserted += inserted
                print(f"    → inserted {inserted} new rows.")
        except Exception as e:
            print(
                f"  ✗  Skipping {commodity or 'ALL'} in {state or 'ALL'}: "
                f"{type(e).__name__}: {e}"
            )
            failed_targets.append((state, commodity))

    # Final summary
    with engine.connect() as conn:
        grand_total = conn.execute(
            text(f"SELECT COUNT(*) FROM {RAW_SCHEMA}.{RAW_TABLE}")
        ).scalar()

    print()
    print(f"Fetched: {total_fetched} records across {len(TARGETS)} targets.")
    print(f"Inserted: {total_inserted} new rows (rest were duplicates).")
    print(f"Total rows in {RAW_SCHEMA}.{RAW_TABLE}: {grand_total}.")
    if failed_targets:
        print(f"Failed targets ({len(failed_targets)}): {failed_targets}")
        return 1  # non-zero exit code so CI/CD can detect partial failure
    return 0


if __name__ == "__main__":
    sys.exit(main())