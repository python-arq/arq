from time import time

import pytest
from pytest_toolbox.comparison import AnyInt, CloseToNow

from arq.connections import ArqRedis
from arq.jobs import Job, JobStatus
from arq.worker import Worker, func


async def test_enqueue_job(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        return 42

    j = await arq_redis.enqueue_job('foobar')
    assert isinstance(j, Job)
    assert JobStatus.queued == await j.status()
    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    assert worker.jobs_complete == 0
    await worker.arun()
    r = await j.result()
    assert r == 42
    assert JobStatus.complete == await j.status()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0


async def test_job_error(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        raise RuntimeError('foobar error')

    j = await arq_redis.enqueue_job('foobar')
    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    await worker.arun()

    with pytest.raises(RuntimeError, match='foobar error'):
        await j.result()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0


async def test_job_info(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        return 42

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
