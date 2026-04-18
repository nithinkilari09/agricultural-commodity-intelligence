"""Quick inspection of what's in the SQLite database."""

import sqlite3
import pandas as pd

DB_PATH = "data/mandi_prices.db"

conn = sqlite3.connect(DB_PATH)

# How many rows total?
total = conn.execute("SELECT COUNT(*) FROM mandi_prices").fetchone()[0]
print(f"Total rows: {total}\n")

# First 5 rows, nicely formatted
df = pd.read_sql("SELECT * FROM mandi_prices LIMIT 5", conn)
print("Sample records:")
print(df.to_string(index=False))
print()

# Price summary
summary = pd.read_sql(
    """
    SELECT
        commodity,
        COUNT(*) AS records,
        ROUND(AVG(modal_price), 2) AS avg_modal_price,
        MIN(min_price) AS lowest_min,
        MAX(max_price) AS highest_max
    FROM mandi_prices
    GROUP BY commodity
    """,
    conn,
)
print("Price summary:")
print(summary.to_string(index=False))
print()

# How many distinct markets?
markets = conn.execute(
    "SELECT COUNT(DISTINCT market) FROM mandi_prices"
).fetchone()[0]
print(f"Distinct markets: {markets}")

# Date range
date_range = conn.execute(
    "SELECT MIN(arrival_date), MAX(arrival_date) FROM mandi_prices"
).fetchone()
print(f"Date range: {date_range[0]} to {date_range[1]}")

conn.close()