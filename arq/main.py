"""
:mod:`main`
===========

Defines the main ``Actor`` class and ``@concurrent`` decorator for using arq from within your code.

Also defines the ``@cron`` decorator for declaring cron job functions.
"""
import asyncio
import inspect
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union, cast  # noqa

from .jobs import Job
from .utils import RedisMixin, next_cron, to_unix_ms

__all__ = ['Actor', 'concurrent', 'cron']

main_logger = logging.getLogger('arq.main')
