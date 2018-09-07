# Configure the scheduler here, it will then be imported into the orchestrator.

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import utc


# Initial Configuration of the Scheduler
job_defaults = {
    'coalesce': False,
    'max_instances': 10,
    'trigger': 'cron',
    'replace_existing': True,
}
scheduler = AsyncIOScheduler()
scheduler.configure(job_defaults=job_defaults, timezone=utc)
