"""APScheduler jobs for automatic data refresh."""
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.agents.orchestrator import run_pipeline

logger = logging.getLogger(__name__)


async def _morning_run():
    """Daily morning pipeline run at 08:00."""
    logger.info("Morning pipeline run triggered (08:00)")
    await run_pipeline(force=True)


async def _midday_run():
    """Daily midday pipeline run at 15:00."""
    logger.info("Midday pipeline run triggered (15:00)")
    await run_pipeline(force=True)


async def _evening_run():
    """Daily evening pipeline run at 22:00."""
    logger.info("Evening pipeline run triggered (22:00)")
    await run_pipeline(force=True)


def create_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="Asia/Jerusalem")

    # Morning run 08:00 daily
    scheduler.add_job(
        _morning_run,
        trigger=CronTrigger(hour=8, minute=0),
        id="morning_run",
        name="Daily 08:00 morning refresh",
        replace_existing=True,
    )

    # Midday run 15:00 daily
    scheduler.add_job(
        _midday_run,
        trigger=CronTrigger(hour=15, minute=0),
        id="midday_run",
        name="Daily 15:00 midday refresh",
        replace_existing=True,
    )

    # Evening run 22:00 daily (2h after Shufersal ~20:00 update)
    scheduler.add_job(
        _evening_run,
        trigger=CronTrigger(hour=22, minute=0),
        id="evening_run",
        name="Daily 22:00 evening refresh",
        replace_existing=True,
    )

    return scheduler
