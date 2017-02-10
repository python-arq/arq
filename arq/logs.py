"""
:mod:`logs`
===========
"""
import logging
import re

import click

__all__ = ['ColourHandler', 'default_log_config']

LOG_COLOURS = {
    logging.DEBUG: 'white',
    logging.INFO: 'green',
    logging.WARN: 'yellow',
}


class ColourHandler(logging.StreamHandler):
    """
    Coloured log handler. Levels: debug: white, info: green, warning: yellow, else: red.

    Date times (anything before the first colon) is magenta.
    """
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


def default_log_config(verbose: bool) -> dict:
    """
    Setup default config. for dictConfig.

    :param verbose: level: DEBUG if True, INFO if False
    :return: dict suitable for ``logging.config.dictConfig``
    """
    log_level = 'DEBUG' if verbose else 'INFO'
    return {
        'version': 1,
        'disable_existing_loggers': False,
        'handlers': {
            'arq.colour': {
                'level': log_level,
                'class': 'arq.logs.ColourHandler',
                'formatter': 'arq.standard'
            },
        },
        'formatters': {
            'arq.standard': {
                'format': '%(asctime)s %(processName)11s: %(message)s',
                'datefmt': '%H:%M:%S',
            },
        },
        'loggers': {
            'arq': {
                'handlers': ['arq.colour'],
                'level': log_level,
            }
        },
    }
