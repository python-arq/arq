import asyncio

import pytest
from pytest_toolbox.comparison import CloseToNow

from arq import Worker, func
from arq.connections import ArqRedis
from arq.constants import in_progress_key_prefix
from arq.jobs import Job, JobResult, JobStatus, pickle_result


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
    assert info == JobResult(
        job_try=1,
        function='foobar',
        args=(1, 2),
        kwargs={'c': 3},
        enqueue_time=CloseToNow(),
        success=True,
        result=42,
        start_time=CloseToNow(),
        finish_time=CloseToNow(),
        score=None,
    )
    results = await arq_redis.all_job_results()
    assert results == [
        JobResult(
            function='foobar',
            args=(1, 2),
            kwargs={'c': 3},
            job_try=1,
            enqueue_time=CloseToNow(),
            success=True,
            result=42,
            start_time=CloseToNow(),
            finish_time=CloseToNow(),
            score=None,
            job_id=j.job_id,
        )
    ]


async def test_cant_unpickel_at_all():
    class Foobar:
        def __getstate__(self):
            raise TypeError("this doesn't pickle")

    r1 = pickle_result('foobar', (1,), {}, 1, 123, True, Foobar(), 123, 123, 'testing')
    assert isinstance(r1, bytes)
    r2 = pickle_result('foobar', (Foobar(),), {}, 1, 123, True, Foobar(), 123, 123, 'testing')
    assert r2 is None
