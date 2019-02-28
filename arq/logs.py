"""
:mod:`logs`
===========
"""
import logging
import re

import click

__all__ = ['ColourHandler', 'default_log_config']

LOG_FORMATS = {
    logging.DEBUG: {'fg': 'white', 'dim': True},
    logging.INFO: {'fg': 'green'},
    logging.WARN: {'fg': 'yellow'},
}


def get_log_format(record):
    return LOG_FORMATS.get(record.levelno, {'fg': 'red'})


class ColourHandler(logging.StreamHandler):
    pass


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
