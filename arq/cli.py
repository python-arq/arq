import asyncio
import logging.config
import os
import sys
from signal import Signals

import click
from pydantic.utils import import_string

from .logs import default_log_config
from .version import VERSION
from .worker import check_health, create_worker, run_worker

burst_help = 'Batch mode: exit once no jobs are found in any queue.'
health_check_help = 'Health Check: run a health check and exit.'
watch_help = 'Watch a directory and reload the worker upon changes.'
verbose_help = 'Enable verbose output.'


@click.command('arq')
@click.version_option(VERSION, '-V', '--version', prog_name='arq')
@click.argument('worker-settings', type=str, required=True)
@click.option('--burst/--no-burst', default=None, help=burst_help)
@click.option('--check', is_flag=True, help=health_check_help)
@click.option('--watch', type=click.Path(exists=True, dir_okay=True, file_okay=False), help=watch_help)
@click.option('-v', '--verbose', is_flag=True, help=verbose_help)
def cli(*, worker_settings, burst, check, watch, verbose):
    """
    Job queues in python with asyncio and redis.

    CLI to run the arq worker.
    """
    sys.path.append(os.getcwd())
    worker_settings = import_string(worker_settings)
    logging.config.dictConfig(default_log_config(verbose))

    if check:
        exit(check_health(worker_settings))
    else:
        kwargs = {} if burst is None else {'burst': burst}
        if watch:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(watch_reload(watch, worker_settings, loop))
        else:
            run_worker(worker_settings, **kwargs)


async def watch_reload(path, worker_settings, loop):
    try:
        from watchgod import awatch
    except ImportError as e:  # pragma: no cover
        raise ImportError('watchgod not installed, use `pip install watchgod`') from e

    stop_event = asyncio.Event()
    worker = create_worker(worker_settings)
    try:
        worker.on_stop = lambda s: s != Signals.SIGUSR1 and stop_event.set()
        loop.create_task(worker.async_run())
        async for _ in awatch(path, stop_event=stop_event):
            print('\nfiles changed, reloading arq worker...')
            worker.handle_sig(Signals.SIGUSR1)
            await worker.close()
            loop.create_task(worker.async_run())
    finally:
        await worker.close()
