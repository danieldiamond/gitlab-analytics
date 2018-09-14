import logging

from fire import Fire

from env_var_checker import env_vars
from orchestrator import run_scheduler

def start_scheduler():
    logging.info('Starting scheduler...')
    run_scheduler(env_vars)

if __name__ == '__main__' and __package__ is None:
    Fire({'scheduler': start_scheduler})

