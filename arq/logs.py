"""
:mod:`logs`
===========
"""
import logging
from typing import Dict, Union

import re

import click

__all__ = ['ColourHandler', 'default_log_config']

LOG_FORMATS: Dict[int, Dict[str, Union[str, bool]]] = {
    logging.DEBUG: {'fg': 'white', 'dim': True},
    logging.INFO: {'fg': 'green'},
    logging.WARN: {'fg': 'yellow'},
}


def get_log_format(record: logging.LogRecord) -> Dict[str, Union[str, bool]]:
    return LOG_FORMATS.get(record.levelno, {'fg': 'red'})


class ColourHandler(logging.StreamHandler):
    """
    Coloured log handler. Levels: debug: white, info: green, warning: yellow, else: red.

    Date times (anything before the first colon) is magenta.
    """
    def emit(self, record: logging.LogRecord) -> None:
        log_entry = self.format(record)
        m = re.match('^(.*?: )', log_entry)
        if m:
            prefix = click.style(m.groups()[0], fg='magenta')
            msg = click.style(log_entry[m.end():], **get_log_format(record))  # type: ignore
            click.echo(prefix + msg)
        else:
            click.secho(log_entry, **get_log_format(record))  # type: ignore


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
