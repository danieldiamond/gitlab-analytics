# Configure the scheduler here, it will then be imported into the orchestrator.
import logging
from os import getenv
import time

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from pytz import utc
from sqlalchemy import create_engine


conn_string = getenv('SCHED_CONN_STRING', 'sqlite:///jobs.sqlite')
if getenv('SCHED_CONN_STRING'):
    engine = create_engine(conn_string)
    while True:
        time.sleep(5)
        try:
            connection = engine.connect()
            connection.close()
            logging.info('Connected to database.')
            break
        except:
            logging.warning("Can't connect to database, trying again...")


# Initial Configuration of the Scheduler
jobstores = {
        # default to sqlite if no other job store is specified
        'default': SQLAlchemyJobStore(
                        url=getenv('SCHED_CONN_STRING', 'sqlite:///jobs.sqlite'),
                        tablename='orchestrator_jobs',
                        tableschema='public')
}
job_defaults = {
    'max_instances': getenv('SCHED_MAX_INSTANCES', 10),
    'coalesce': False,
    'trigger': 'cron',
    'replace_existing': True,
}
scheduler = AsyncIOScheduler()
scheduler.configure(jobstores=jobstores, job_defaults=job_defaults, timezone=utc)
