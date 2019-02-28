import asyncio
import logging
from datetime import datetime, timedelta
from typing import Union

from aioredis import MultiExecError

from .connections import ArqRedis
from .constants import cron_key_prefix
from .utils import to_unix_ms

logger = logging.getLogger('arq.cron')

_dt_fields = [
    'month',
    'day',
    'weekday',
    'hour',
    'minute',
    'second',
    'microsecond',
]


async def run_cron(redis_pool: ArqRedis, con_jobs):
    n = datetime.now()
    to_run = set()

    for cron_job in con_jobs:
        if n >= cron_job.next_run:
            to_run.add((cron_job, cron_job.next_run))
            cron_job.set_next(n)

    if not to_run:
        return

    logger.debug('cron, %d jobs to run', len(to_run))
    job_futures = set()
    for cron_job, run_at in to_run:
        if cron_job.unique:
            key = cron_key_prefix + cron_job.name
            value = str(to_unix_ms(run_at))
            with await redis_pool as conn:
                _, _, existing_value = await asyncio.gather(
                    conn.unwatch(),
                    conn.watch(key),
                    conn.get(key),
                )
                if existing_value == value:
                    # another worker has already set it and is doing this cron job
                    continue

                tr = conn.multi_exec()
                tr.setex(key, 3600, b'1')
                try:
                    await tr.execute()
                except MultiExecError:
                    continue

        job_futures.add(redis_pool.enqueue_job(cron_job.name))

    job_futures and await asyncio.gather(*job_futures)


def _get_next_dt(dt_, options):  # noqa: C901
    for field in _dt_fields:
        v = options[field]
        if v is None:
            continue
        if field == 'weekday':
            next_v = dt_.weekday()
        else:
            next_v = getattr(dt_, field)
        if isinstance(v, int):
            mismatch = next_v != v
        else:
            assert isinstance(v, (set, list, tuple))
            mismatch = next_v not in v
        # print(field, v, next_v, mismatch)
        if mismatch:
            micro = max(dt_.microsecond - options['microsecond'], 0)
            if field == 'month':
                if dt_.month == 12:
                    return datetime(dt_.year + 1, 1, 1)
                else:
                    return datetime(dt_.year, dt_.month + 1, 1)
            elif field in ('day', 'weekday'):
                return dt_ + timedelta(days=1) - timedelta(hours=dt_.hour, minutes=dt_.minute, seconds=dt_.second,
                                                           microseconds=micro)
            elif field == 'hour':
                return dt_ + timedelta(hours=1) - timedelta(minutes=dt_.minute, seconds=dt_.second, microseconds=micro)
            elif field == 'minute':
                return dt_ + timedelta(minutes=1) - timedelta(seconds=dt_.second, microseconds=micro)
            elif field == 'second':
                return dt_ + timedelta(seconds=1) - timedelta(microseconds=micro)
            else:
                assert field == 'microsecond'
                return dt_ + timedelta(microseconds=options['microsecond'] - dt_.microsecond)


def next_cron(preview_dt: datetime, *,
              month: Union[None, set, int] = None,
              day: Union[None, set, int] = None,
              weekday: Union[None, set, int, str] = None,
              hour: Union[None, set, int] = None,
              minute: Union[None, set, int] = None,
              second: Union[None, set, int] = 0,
              microsecond: int = 123456):
    """
    Find the next datetime matching the given parameters.
    """
    dt = preview_dt + timedelta(seconds=1)
    if isinstance(weekday, str):
        weekday = ['mon', 'tues', 'wed', 'thurs', 'fri', 'sat', 'sun'].index(weekday.lower())
    options = dict(
        month=month,
        day=day,
        weekday=weekday,
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond,
    )

    while True:
        next_dt = _get_next_dt(dt, options)
        # print(dt, next_dt)
        if next_dt is None:
            return dt
        dt = next_dt
