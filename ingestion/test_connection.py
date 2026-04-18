"""
One-off connectivity test: can Python reach our Supabase Postgres?

This does nothing useful — it just connects, runs SELECT 1, and
prints the result. If this works, our credentials and network
path are good. If it fails, we fix that before touching any
real code.
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError("DATABASE_URL not found in .env")

# SQLAlchemy needs to know which Postgres driver to use. Our .env
# holds the standard Postgres URL (from Supabase); we tell SQLAlchemy
# to use psycopg3 by injecting the driver name into the URL scheme.
if db_url.startswith("postgresql://"):
    db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)

print("Connecting to database...")
engine = create_engine(db_url)
with engine.connect() as conn:
    result = conn.execute(text("SELECT version()"))
    version = result.scalar()
    print(f"✓ Connected successfully.")
    print(f"Postgres version: {version}")

    # Bonus: confirm we're in the right database
    db_name = conn.execute(text("SELECT current_database()")).scalar()
    user = conn.execute(text("SELECT current_user")).scalar()
    print(f"Database: {db_name}")
    print(f"User: {user}")