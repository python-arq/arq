import logging.config

import click
from pydantic.utils import import_string

from .logs import default_log_config
from .version import VERSION
from .worker import check_health, run_worker

burst_help = 'Batch mode: exit once no jobs are found in any queue.'
health_check_help = 'Health Check: run a health check and exit'
verbose_help = 'Enable verbose output.'


@click.command('arq')
@click.version_option(VERSION, '-V', '--version', prog_name='arq')
@click.argument('worker-settings', type=str, required=True)
@click.option('--burst/--no-burst', default=None, help=burst_help)
@click.option('--check', is_flag=True, help=health_check_help)
@click.option('-v', '--verbose', is_flag=True, help=verbose_help)
def cli(*, worker_settings, burst, check, verbose):
    """
    Job queues in python with asyncio and redis.

    CLI to run the arq worker.
    """
    worker_settings = import_string(worker_settings)
    logging.config.dictConfig(default_log_config(verbose))

    if check:
        exit(check_health(worker_settings))
    else:
        kwargs = {} if burst is None else {'burst': burst}
        run_worker(worker_settings, **kwargs)
