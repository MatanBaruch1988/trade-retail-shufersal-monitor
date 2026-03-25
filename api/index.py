"""Vercel serverless entry point for the FastAPI backend."""
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Override DB path to use bundled read-only snapshot
os.environ.setdefault("DB_PATH", os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "prices.db"
))

from mangum import Mangum
from backend.main import app

handler = Mangum(app, lifespan="off")
