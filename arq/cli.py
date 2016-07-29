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


class DefaultHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        colour = LOG_COLOURS.get(record.levelno, 'red')
        m = re.match('^(\[.*?\])', log_entry)
        if m:
            time = click.style(m.groups()[0], fg='magenta')
            msg = click.style(log_entry[m.end():], fg=colour)
            click.echo(time + msg)
        else:
            click.secho(log_entry, fg=colour)


def setup_logging(verbose=False):
    log_level = logging.DEBUG if verbose else logging.INFO
    formatter = logging.Formatter('[%(asctime)s] %(processName)8s %(name)8s: %(message)s', datefmt='%H:%M:%S')
    dft_hdl = DefaultHandler()
    dft_hdl.setFormatter(formatter)
    for name in ['arq.work']:
        logger = logging.getLogger(name)
        for h in logger.handlers:
            logger.removeHandler(h)
        logger.addHandler(dft_hdl)
        logger.setLevel(log_level)


batch_help = 'Batch mode: exit once no jobs are found in any queue.'
verbose_help = 'Enable verbose output.'


@click.group()
def cli():
    """
    Job queues in python with asyncio and redis.
    """
    pass


@cli.command()
@click.version_option(VERSION, '-V', '--version', prog_name='arq')
@click.argument('worker-path', type=click.Path(exists=True, dir_okay=False, file_okay=True), required=True)
@click.argument('worker-class', default='Worker')
@click.option('--batch/--no-batch', default=False, help=batch_help)
@click.option('-v', '--verbose', is_flag=True, help=verbose_help)
def worker(worker_path, worker_class, batch, verbose):
    """
    Run the arq worker.
    """
    setup_logging(verbose)
    RunWorkerProcess(worker_path, worker_class, batch)
