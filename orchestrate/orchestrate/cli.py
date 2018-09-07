import logging

from fire import Fire

import env_var_checker
from orchestrator import run_scheduler

def start_scheduler():
    logging.info('Starting scheduler...')
    run_scheduler()

if __name__ == '__main__' and __package__ is None:
    Fire({'scheduler': start_scheduler})

