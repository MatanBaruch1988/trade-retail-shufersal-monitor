"""Entry point for GitHub Actions pipeline run.

Usage:
    python run_pipeline_job.py

Required env vars:
    DATABASE_URL      - Supabase PostgreSQL connection string
    ANTHROPIC_API_KEY - Claude API key for AI analysis
"""
import asyncio
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)


async def main() -> None:
    from backend.agents.orchestrator import run_pipeline
    result = await run_pipeline(force=False, trigger="github-actions")
    logging.info("Pipeline finished: %s", result)
    if result.get("status") == "failed":
        sys.exit(1)


asyncio.run(main())
