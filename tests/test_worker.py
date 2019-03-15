import asyncio
import logging
import re
import signal
from unittest.mock import MagicMock

import pytest
from aioredis import create_redis_pool

from arq.connections import ArqRedis
from arq.constants import health_check_key, job_key_prefix
from arq.worker import FailedJobs, Retry, Worker, async_check_health, check_health, func, run_worker


async def foobar(ctx):
    return 42


async def fails(ctx):
    raise TypeError('xxx')


def test_no_jobs(arq_redis: ArqRedis, loop):
    class Settings:
        functions = [func(foobar, name='foobar')]
        burst = True
        poll_delay = 0

    loop.run_until_complete(arq_redis.enqueue_job('foobar'))
    worker = run_worker(Settings)
    assert worker.jobs_complete == 1
    assert str(worker) == '<Worker j_complete=1 j_failed=0 j_retried=0 j_ongoing=0>'


def test_health_check_direct(loop):
    class Settings:
        pass

    assert check_health(Settings) == 1


async def test_health_check_fails():
    assert 1 == await async_check_health(None)


async def test_health_check_pass(arq_redis):
    await arq_redis.set(health_check_key, b'1')
    assert 0 == await async_check_health(None)


async def test_handle_sig(caplog):
    caplog.set_level(logging.INFO)
    worker = Worker([foobar])
    worker.main_task = MagicMock()
    worker.tasks = [MagicMock(done=MagicMock(return_value=True)), MagicMock(done=MagicMock(return_value=False))]

    assert len(caplog.records) == 0
    worker.handle_sig(signal.SIGINT)
    assert len(caplog.records) == 1
    assert caplog.records[0].message == (
        'shutdown on SIGINT ◆ 0 jobs complete ◆ 0 failed ◆ 0 retries ◆ 2 ongoing to cancel'
    )
    assert worker.main_task.cancel.call_count == 1
    assert worker.tasks[0].done.call_count == 1
    assert worker.tasks[0].cancel.call_count == 0
    assert worker.tasks[1].done.call_count == 1
    assert worker.tasks[1].cancel.call_count == 1


async def test_job_successful(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    worker: Worker = worker(functions=[foobar])
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'X.XXs → testing:foobar()\n  X.XXs ← testing:foobar ● 42' in log


async def test_job_retry(arq_redis: ArqRedis, worker, caplog):
    async def retry(ctx):
        if ctx['job_try'] <= 2:
            raise Retry(defer=0.01)

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')])
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 2

    log = re.sub(r'(\d+).\d\ds', r'\1.XXs', '\n'.join(r.message for r in caplog.records))
    assert '0.XXs ↻ testing:retry retrying job in 0.XXs\n' in log
    assert '0.XXs → testing:retry() try=2\n' in log
    assert '0.XXs ← testing:retry ●' in log


async def test_job_job_not_found(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('missing', _job_id='testing')
    worker: Worker = worker(functions=[foobar])
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0

    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert "job testing, function 'missing' not found" in log


async def test_retry_lots(arq_redis: ArqRedis, worker, caplog):
    async def retry(ctx):
        raise Retry()

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')])
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 5

    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert '  X.XXs ! testing:retry max retries 5 exceeded' in log


async def test_cancel_error(arq_redis: ArqRedis, worker, caplog):
    async def retry(ctx):
        if ctx['job_try'] == 1:
            raise asyncio.CancelledError()

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')])
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 1

    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'X.XXs ↻ testing:retry cancelled, will be run again' in log


async def test_job_expired(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    await arq_redis.delete(job_key_prefix + 'testing')
    worker: Worker = worker(functions=[foobar])
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0

    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'job testing expired' in log


async def test_job_old(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foobar', _job_id='testing', _defer_by=-2)
    worker: Worker = worker(functions=[foobar])
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    log = re.sub(r'(\d+).\d\ds', r'\1.XXs', '\n'.join(r.message for r in caplog.records))
    assert log.endswith('  0.XXs → testing:foobar() delayed=2.XXs\n' '  0.XXs ← testing:foobar ● 42')


async def test_retry_repr():
    assert str(Retry(123)) == '<Retry defer 123.00s>'


async def test_str_function(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('asyncio.sleep', _job_id='testing')
    worker: Worker = worker(functions=['asyncio.sleep'])
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0

    log = re.sub(r'(\d+).\d\ds', r'\1.XXs', '\n'.join(r.message for r in caplog.records))
    assert '0.XXs ! testing:asyncio.sleep failed, TypeError' in log


async def test_startup_shutdown(arq_redis: ArqRedis, worker):
    calls = []

    async def startup(ctx):
        calls.append('startup')

    async def shutdown(ctx):
        calls.append('shutdown')

    await arq_redis.enqueue_job('foobar', _job_id='testing')
    worker: Worker = worker(functions=[foobar], on_startup=startup, on_shutdown=shutdown)
    await worker.main()
    await worker.close()

    assert calls == ['startup', 'shutdown']


class CustomError(RuntimeError):
    def extra(self):
        return {'x': 'y'}


async def error_function(ctx):
    raise CustomError('this is the error')


async def test_exc_extra(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('error_function', _job_id='testing')
    worker: Worker = worker(functions=[error_function])
    await worker.main()
    assert worker.jobs_failed == 1

    log = re.sub(r'(\d+).\d\ds', r'\1.XXs', '\n'.join(r.message for r in caplog.records))
    assert '0.XXs ! testing:error_function failed, CustomError: this is the error' in log
    error = next(r for r in caplog.records if r.levelno == logging.ERROR)
    assert error.extra == {'x': 'y'}


async def test_unpickleable(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)

    class Foo:
        pass

    async def example(ctx):
        return Foo()

    await arq_redis.enqueue_job('example', _job_id='testing')
    worker: Worker = worker(functions=[func(example, name='example')])
    await worker.main()

    log = re.sub(r'(\d+).\d\ds', r'\1.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'error pickling result of testing:example' in log


async def test_log_health_check(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    worker: Worker = worker(functions=[foobar], health_check_interval=0)
    await worker.main()
    await worker.main()
    await worker.main()
    assert worker.jobs_complete == 1

    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'j_complete=1 j_failed=0 j_retried=0 j_ongoing=0 queued=0' in log
    assert log.count('recording health') == 1


async def test_remain_keys(arq_redis: ArqRedis, worker):
    redis2 = await create_redis_pool(('localhost', 6379), encoding='utf8')
    try:
        await arq_redis.enqueue_job('foobar', _job_id='testing')
        assert sorted(await redis2.keys('*')) == ['arq:job:testing', 'arq:queue']
        worker: Worker = worker(functions=[foobar])
        await worker.main()
        assert sorted(await redis2.keys('*')) == ['arq:health-check', 'arq:result:testing']
        await worker.close()
        assert sorted(await redis2.keys('*')) == ['arq:result:testing']
    finally:
        redis2.close()
        await redis2.wait_closed()


async def test_remain_keys_no_results(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    assert sorted(await arq_redis.keys('*')) == ['arq:job:testing', 'arq:queue']
    worker: Worker = worker(functions=[func(foobar, keep_result=0)])
    await worker.main()
    assert sorted(await arq_redis.keys('*')) == ['arq:health-check']


async def test_run_check_passes(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('foobar')
    await arq_redis.enqueue_job('foobar')
    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    assert 2 == await worker.run_check()


async def test_run_check_error(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('fails')
    worker: Worker = worker(functions=[func(fails, name='fails')])
    with pytest.raises(FailedJobs, match='1 job failed'):
        await worker.run_check()


async def test_run_check_error2(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('fails')
    await arq_redis.enqueue_job('fails')
    worker: Worker = worker(functions=[func(fails, name='fails')])
    with pytest.raises(FailedJobs, match='2 jobs failed') as exc_info:
        await worker.run_check()
    assert len(exc_info.value.job_results) == 2


async def test_return_exception(arq_redis: ArqRedis, worker):
    async def return_error(ctx):
        return TypeError('xxx')

    j = await arq_redis.enqueue_job('return_error')
    worker: Worker = worker(functions=[func(return_error, name='return_error')])
    await worker.async_run()
    assert (worker.jobs_complete, worker.jobs_failed, worker.jobs_retried) == (1, 0, 0)
    r = await j.result(pole_delay=0)
    assert isinstance(r, TypeError)
    info = await j.result_info()
    assert info['success'] is True


async def test_error_success(arq_redis: ArqRedis, worker):
    j = await arq_redis.enqueue_job('fails')
    worker: Worker = worker(functions=[func(fails, name='fails')])
    await worker.async_run()
    assert (worker.jobs_complete, worker.jobs_failed, worker.jobs_retried) == (0, 1, 0)
    info = await j.result_info()
    assert info['success'] is False
