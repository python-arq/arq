import logging.config

import click

from .version import VERSION
from .worker import RunWorkerProcess, import_string

burst_help = 'Batch mode: exit once no jobs are found in any queue.'
verbose_help = 'Enable verbose output.'


@click.command()
@click.version_option(VERSION, '-V', '--version', prog_name='arq')
@click.argument('worker-path', type=click.Path(exists=True, dir_okay=False, file_okay=True), required=True)
@click.argument('worker-class', default='Worker')
@click.option('--burst/--no-burst', default=False, help=burst_help)
@click.option('-v', '--verbose', is_flag=True, help=verbose_help)
def cli(*, worker_path, worker_class, burst, verbose):
    """
    Job queues in python with asyncio, redis and msgpack.

    CLI to run the arq worker.
    """
    worker = import_string(worker_path, worker_class)
    logging.config.dictConfig(worker.logging_config(verbose))

    RunWorkerProcess(worker_path, worker_class, burst)
