from datetime import datetime
from time import time

import pytest
from pytest_toolbox.comparison import AnyInt, CloseToNow

from arq.connections import ArqRedis
from arq.constants import queue_name
from arq.jobs import Job, JobStatus
from arq.utils import timestamp_ms
from arq.worker import Worker, func


async def test_enqueue_job(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        return 42

    j = await arq_redis.enqueue_job('foobar')
    assert isinstance(j, Job)
    assert JobStatus.queued == await j.status()
    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    await worker.arun()
    r = await j.result()
    assert r == 42
    assert JobStatus.complete == await j.status()


async def test_job_error(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        raise RuntimeError('foobar error')

    j = await arq_redis.enqueue_job('foobar')
    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    await worker.arun()

    with pytest.raises(RuntimeError, match='foobar error'):
        await j.result()


async def test_job_info(arq_redis: ArqRedis):
    t_before = time()
    j = await arq_redis.enqueue_job('foobar', 123, a=456)
    info = await j.info()
    assert info == {
        'enqueue_time': CloseToNow(),
        'function': 'foobar',
        'args': (123,),
        'kwargs': {'a': 456},
        'score': AnyInt(),
    }
    assert abs(t_before * 1000 - info['score']) < 1000


async def test_repeat_job(arq_redis: ArqRedis):
    j1 = await arq_redis.enqueue_job('foobar', _job_id='job_id')
    assert isinstance(j1, Job)
    j2 = await arq_redis.enqueue_job('foobar', _job_id='job_id')
    assert j2 is None


async def test_defer_until(arq_redis: ArqRedis):
    j1 = await arq_redis.enqueue_job('foobar', _job_id='job_id', _defer_until=datetime(2032, 1, 1))
    assert isinstance(j1, Job)
    score = await arq_redis.zscore(queue_name, 'job_id')
    assert score == 1_956_528_000_000


async def test_defer_by(arq_redis: ArqRedis):
    j1 = await arq_redis.enqueue_job('foobar', _job_id='job_id', _defer_by=20)
    assert isinstance(j1, Job)
    score = await arq_redis.zscore(queue_name, 'job_id')
    ts = timestamp_ms()
    assert score > ts + 19000
    assert ts + 21000 > score
