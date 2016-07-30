import logging
import re

import click

__all__ = ['ClickHandler', 'default_log_config']

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
        if m:
            prefix = click.style(m.groups()[0], fg='magenta')
            msg = click.style(log_entry[m.end():], fg=colour)
            click.echo(prefix + msg)
        else:
            click.secho(log_entry, fg=colour)


def default_log_config(verbose):
    log_level = 'DEBUG' if verbose else 'INFO'
    return {
        'version': 1,
        'disable_existing_loggers': True,
        'handlers': {
            'click': {
                'level': log_level,
                'class': 'arq.logs.ClickHandler',
                'formatter': 'standard'
            },
        },
        'formatters': {
            'standard': {
                'format': '%(asctime)s %(processName)11s: %(message)s',
                'datefmt': '%H:%M:%S',
            },
        },
        'loggers': {
            'arq.main': {
                'handlers': ['click'],
                'level': log_level,
            },
            'arq.work': {
                'handlers': ['click'],
                'level': log_level,
            },
        },
    }
