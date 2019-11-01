import logging
import re
from datetime import datetime, timedelta
from random import random

import pytest

from arq import Worker
from arq.cron import cron, next_cron


@pytest.mark.parametrize(
    'previous,expected,kwargs',
    [
        (datetime(2016, 6, 1, 12, 10, 10), datetime(2016, 6, 1, 12, 10, 20, microsecond=123_456), dict(second=20)),
        (datetime(2016, 6, 1, 12, 10, 10), datetime(2016, 6, 1, 12, 11, 0, microsecond=123_456), dict(minute=11)),
        (datetime(2016, 6, 1, 12, 10, 10), datetime(2016, 6, 1, 12, 10, 20), dict(second=20, microsecond=0)),
        (datetime(2016, 6, 1, 12, 10, 10), datetime(2016, 6, 1, 12, 11, 0), dict(minute=11, microsecond=0)),
        (
            datetime(2016, 6, 1, 12, 10, 11),
            datetime(2017, 6, 1, 12, 10, 10, microsecond=123_456),
            dict(month=6, day=1, hour=12, minute=10, second=10),
        ),
        (
            datetime(2016, 6, 1, 12, 10, 10, microsecond=1),
            datetime(2016, 7, 1, 12, 10, 10),
            dict(day=1, hour=12, minute=10, second=10, microsecond=0),
        ),
        (datetime(2032, 1, 31, 0, 0, 0), datetime(2032, 2, 28, 0, 0, 0, microsecond=123_456), dict(day=28)),
        (datetime(2032, 1, 1, 0, 5), datetime(2032, 1, 1, 4, 0, microsecond=123_456), dict(hour=4)),
        (datetime(2032, 1, 1, 0, 0), datetime(2032, 1, 1, 4, 2, microsecond=123_456), dict(hour=4, minute={2, 4, 6})),
        (datetime(2032, 1, 1, 0, 5), datetime(2032, 1, 1, 4, 2, microsecond=123_456), dict(hour=4, minute={2, 4, 6})),
        (datetime(2032, 2, 5, 0, 0, 0), datetime(2032, 3, 31, 0, 0, 0, microsecond=123_456), dict(day=31)),
        (
            datetime(2001, 1, 1, 0, 0, 0),  # Monday
            datetime(2001, 1, 7, 0, 0, 0, microsecond=123_456),
            dict(weekday='Sun'),  # Sunday
        ),
        (datetime(2001, 1, 1, 0, 0, 0), datetime(2001, 1, 7, 0, 0, 0, microsecond=123_456), dict(weekday=6)),  # Sunday
        (datetime(2001, 1, 1, 0, 0, 0), datetime(2001, 11, 7, 0, 0, 0, microsecond=123_456), dict(month=11, weekday=2)),
        (datetime(2001, 1, 1, 0, 0, 0), datetime(2001, 1, 3, 0, 0, 0, microsecond=123_456), dict(weekday='wed')),
    ],
)
def test_next_cron(previous, expected, kwargs):
    start = datetime.now()
    assert next_cron(previous, **kwargs) == expected
    diff = datetime.now() - start
    print(f'{diff.total_seconds() * 1000:0.3f}ms')


def test_next_cron_invalid():
    with pytest.raises(ValueError):
        next_cron(datetime(2001, 1, 1, 0, 0, 0), weekday='monday')


@pytest.mark.parametrize(
    'max_previous,kwargs,expected',
    [
        (1, dict(microsecond=59867), datetime(2001, 1, 1, 0, 0, microsecond=59867)),
        (59, dict(second=28, microsecond=0), datetime(2023, 1, 1, 1, 59, 28)),
        (3600, dict(minute=10, second=20), datetime(2016, 6, 1, 12, 10, 20, microsecond=123_456)),
        (68400, dict(hour=3), datetime(2032, 1, 1, 3, 0, 0, microsecond=123_456)),
        (68400 * 60, dict(day=31, minute=59), datetime(2032, 3, 31, 0, 59, 0, microsecond=123_456)),
        (68400 * 7, dict(weekday='tues', minute=59), datetime(2032, 3, 30, 0, 59, 0, microsecond=123_456)),
        (
            68400 * 175,  # previous friday the 13th is February
            dict(day=13, weekday='fri', microsecond=1),
            datetime(2032, 8, 13, 0, 0, 0, microsecond=1),
        ),
        (68400 * 365, dict(month=10, day=4, hour=23), datetime(2032, 10, 4, 23, 0, 0, microsecond=123_456)),
        (
            1,
            dict(month=1, day=1, hour=0, minute=0, second=0, microsecond=69875),
            datetime(2001, 1, 1, 0, 0, microsecond=69875),
        ),
    ],
)
def test_next_cron_random(max_previous, kwargs, expected):
    for i in range(100):
        previous = expected - timedelta(seconds=0.9 + random() * max_previous)
        v = next_cron(previous, **kwargs)
        diff = v - previous
        if diff > timedelta(seconds=1):
            print(f'previous: {previous}, expected: {expected}, diff: {v - previous}')
            assert v == expected


async def foobar(ctx):
    return 42


async def test_job_successful(worker, caplog):
    caplog.set_level(logging.INFO)
    worker: Worker = worker(cron_jobs=[cron(foobar, hour=1, run_at_startup=True)])
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    log = re.sub(r'(\d+).\d\ds', r'\1.XXs', '\n'.join(r.message for r in caplog.records))
    assert '  0.XXs → cron:foobar()\n  0.XXs ← cron:foobar ● 42' in log


async def test_job_successful_on_specific_queue(worker, caplog):
    caplog.set_level(logging.INFO)
    worker: Worker = worker(queue_name='arq:test-cron-queue', cron_jobs=[cron(foobar, hour=1, run_at_startup=True)])
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    log = re.sub(r'(\d+).\d\ds', r'\1.XXs', '\n'.join(r.message for r in caplog.records))
    assert '  0.XXs → cron:foobar()\n  0.XXs ← cron:foobar ● 42' in log


async def test_not_run(worker, caplog):
    caplog.set_level(logging.INFO)
    worker: Worker = worker(cron_jobs=[cron(foobar, hour=1, run_at_startup=False)])
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    log = '\n'.join(r.message for r in caplog.records)
    assert 'cron:foobar()' not in log


async def test_repr():
    cj = cron(foobar, hour=1, run_at_startup=True)
    assert str(cj).startswith('<CronJob name=cron:foobar coroutine=<function foobar at')


async def test_str_function():
    cj = cron('asyncio.sleep', hour=1, run_at_startup=True)
    assert str(cj).startswith('<CronJob name=cron:asyncio.sleep coroutine=<function sleep at')
