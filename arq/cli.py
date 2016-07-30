import logging
import re

import click

from .version import VERSION
from .worker import RunWorkerProcess

LOG_COLOURS = {
    logging.DEBUG: 'white',
    logging.INFO: 'green',
    logging.WARN: 'yellow',
}


class ClickHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        colour = LOG_COLOURS.get(record.levelno, 'red')
        m = re.match('^(.*?: )', log_entry)
        prefix = click.style(m.groups()[0], fg='magenta')
        msg = click.style(log_entry[m.end():], fg=colour)
        click.echo(prefix + msg)


def setup_logging(verbose=False):
    log_level = logging.DEBUG if verbose else logging.INFO
    formatter = logging.Formatter('%(asctime)s %(processName)11s: %(message)s', datefmt='%H:%M:%S')
    dft_hdl = ClickHandler()
    dft_hdl.setFormatter(formatter)
    logger = logging.getLogger('arq.work')
    for h in logger.handlers:
        logger.removeHandler(h)
    logger.addHandler(dft_hdl)
    logger.setLevel(log_level)


batch_help = 'Batch mode: exit once no jobs are found in any queue.'
verbose_help = 'Enable verbose output.'


@click.command()
@click.version_option(VERSION, '-V', '--version', prog_name='arq')
@click.argument('worker-path', type=click.Path(exists=True, dir_okay=False, file_okay=True), required=True)
@click.argument('worker-class', default='Worker')
@click.option('--batch/--no-batch', default=False, help=batch_help)
@click.option('-v', '--verbose', is_flag=True, help=verbose_help)
def cli(*, worker_path, worker_class, batch, verbose):
    """
    Job queues in python with asyncio, redis and msgpack.

    CLI to run the arq worker.
    """
    setup_logging(verbose)
    RunWorkerProcess(worker_path, worker_class, batch)
