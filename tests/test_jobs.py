import asyncio

import pytest
from pytest_toolbox.comparison import CloseToNow

from arq import Worker, func
from arq.connections import ArqRedis
from arq.constants import in_progress_key_prefix
from arq.jobs import Job, JobStatus


async def test_job_in_progress(arq_redis: ArqRedis):
    await arq_redis.set(in_progress_key_prefix + 'foobar', b'1')
    j = Job('foobar', arq_redis)
    assert JobStatus.in_progress == await j.status()
    assert str(j) == '<arq job foobar>'


async def test_unknown(arq_redis: ArqRedis):
    j = Job('foobar', arq_redis)
    assert JobStatus.not_found == await j.status()
    info = await j.info()
    assert info is None


async def test_result_timeout(arq_redis: ArqRedis):
    j = Job('foobar', arq_redis)
    with pytest.raises(asyncio.TimeoutError):
        await j.result(0.1, pole_delay=0)


async def test_enqueue_job(arq_redis: ArqRedis, worker):
    async def foobar(ctx, *args, **kwargs):
        return 42

    j = await arq_redis.enqueue_job('foobar', 1, 2, c=3)
    assert isinstance(j, Job)
    assert JobStatus.queued == await j.status()
    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    await worker.main()
    r = await j.result(pole_delay=0)
    assert r == 42
    assert JobStatus.complete == await j.status()
    info = await j.info()
    assert info == {
        'enqueue_time': CloseToNow(),
        'job_try': 1,
        'function': 'foobar',
        'args': (1, 2),
        'kwargs': {'c': 3},
        'result': 42,
        'start_time': CloseToNow(),
        'finish_time': CloseToNow(),
        'score': None,
    }
    results = await arq_redis.all_job_results()
    assert results == [
        {
            'enqueue_time': CloseToNow(),
            'job_try': 1,
            'function': 'foobar',
            'args': (1, 2),
            'kwargs': {'c': 3},
            'result': 42,
            'start_time': CloseToNow(),
            'finish_time': CloseToNow(),
            'job_id': j.job_id,
        }
    ]
