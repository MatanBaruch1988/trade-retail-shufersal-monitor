"""Vercel serverless entry point for the FastAPI backend."""
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# When DATABASE_URL is set (PostgreSQL / Supabase), the db layer uses asyncpg.
# When it is not set, fall back to SQLite via the bundled snapshot.
if not os.environ.get("DATABASE_URL"):
    os.environ.setdefault("DB_PATH", os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "prices.db"
    ))

from backend.main import app  # noqa: E402  (ASGI app - @vercel/python detects natively)
