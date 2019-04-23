import asyncio
from collections import Counter
from datetime import datetime
from random import shuffle
from time import time

import pytest
from pytest_toolbox.comparison import CloseToNow

from arq.connections import ArqRedis
from arq.constants import queue_name
from arq.jobs import Job, PickleError
from arq.utils import timestamp_ms
from arq.worker import Retry, Worker, func


async def test_enqueue_job(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        return 42

    j = await arq_redis.enqueue_job('foobar')
    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    await worker.main()
    r = await j.result(pole_delay=0)
    assert r == 42  # 1


async def test_job_error(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        raise RuntimeError('foobar error')

    j = await arq_redis.enqueue_job('foobar')
    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    await worker.main()

    with pytest.raises(RuntimeError, match='foobar error'):
        await j.result(pole_delay=0)


async def test_job_info(arq_redis: ArqRedis):
    t_before = time()
    j = await arq_redis.enqueue_job('foobar', 123, a=456)
    info = await j.info()
    assert info.enqueue_time == CloseToNow()
    assert info.job_try is None
    assert info.function == 'foobar'
    assert info.args == (123,)
    assert info.kwargs == {'a': 456}
    assert abs(t_before * 1000 - info.score) < 1000


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


async def test_mung(arq_redis: ArqRedis, worker):
    """
    check a job can't be enqueued multiple times with the same id
    """
    counter = Counter()

    async def count(ctx, v):
        counter[v] += 1

    tasks = []
    for i in range(50):
        tasks.extend(
            [arq_redis.enqueue_job('count', i, _job_id=f'v-{i}'), arq_redis.enqueue_job('count', i, _job_id=f'v-{i}')]
        )
    shuffle(tasks)
    await asyncio.gather(*tasks)

    worker: Worker = worker(functions=[func(count, name='count')])
    await worker.main()
    assert counter.most_common(1)[0][1] == 1  # no job go enqueued twice


async def test_custom_try(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        return ctx['job_try']

    j1 = await arq_redis.enqueue_job('foobar')
    w: Worker = worker(functions=[func(foobar, name='foobar')])
    await w.main()
    r = await j1.result(pole_delay=0)
    assert r == 1

    j2 = await arq_redis.enqueue_job('foobar', _job_try=3)
    await w.main()
    r = await j2.result(pole_delay=0)
    assert r == 3


async def test_custom_try2(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        if ctx['job_try'] == 3:
            raise Retry()
        return ctx['job_try']

    j1 = await arq_redis.enqueue_job('foobar', _job_try=3)
    w: Worker = worker(functions=[func(foobar, name='foobar')])
    await w.main()
    r = await j1.result(pole_delay=0)
    assert r == 4


async def test_cant_pickle(arq_redis: ArqRedis, worker):
    class Foobar:
        def __getstate__(self):
            raise TypeError("this doesn't pickle")

    async def foobar(ctx):
        return Foobar()

    j1 = await arq_redis.enqueue_job('foobar', _job_try=3)
    w: Worker = worker(functions=[func(foobar, name='foobar')])
    await w.main()
    with pytest.raises(PickleError):
        await j1.result(pole_delay=0)
