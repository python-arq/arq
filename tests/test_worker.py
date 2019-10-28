import asyncio
import functools
import logging
import re
import signal
import sys
from unittest.mock import MagicMock

import msgpack
import pytest
from aioredis import create_redis_pool

from arq.connections import ArqRedis
from arq.constants import default_queue_name, health_check_key_suffix, job_key_prefix
from arq.jobs import Job, JobStatus
from arq.worker import FailedJobs, JobExecutionFailed, Retry, Worker, async_check_health, check_health, func, run_worker


async def foobar(ctx):
    return 42


async def fails(ctx):
    raise TypeError('my type error')


def test_no_jobs(arq_redis: ArqRedis, loop):
    class Settings:
        functions = [func(foobar, name='foobar')]
        burst = True
        poll_delay = 0
        queue_read_limit = 10

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
    await arq_redis.set(default_queue_name + health_check_key_suffix, b'1')
    assert 0 == await async_check_health(None)


async def test_set_health_check_key(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    worker: Worker = worker(functions=[func(foobar, keep_result=0)], health_check_key='arq:test:health-check')
    await worker.main()
    assert sorted(await arq_redis.keys('*')) == ['arq:test:health-check']


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


async def test_job_retry_dont_retry(arq_redis: ArqRedis, worker, caplog):
    async def retry(ctx):
        raise Retry(defer=0.01)

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')])
    with pytest.raises(FailedJobs) as exc_info:
        await worker.run_check(retry_jobs=False)
    assert str(exc_info.value) == '1 job failed <Retry defer 0.01s>'

    assert '↻' not in caplog.text
    assert '! testing:retry failed, Retry: <Retry defer 0.01s>\n' in caplog.text


async def test_job_retry_max_jobs(arq_redis: ArqRedis, worker, caplog):
    async def retry(ctx):
        raise Retry(defer=0.01)

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')])
    assert await worker.run_check(max_burst_jobs=1) == 0
    assert worker.jobs_complete == 0
    assert worker.jobs_retried == 1
    assert worker.jobs_failed == 0

    log = re.sub(r'(\d+).\d\ds', r'\1.XXs', caplog.text)
    assert '0.XXs ↻ testing:retry retrying job in 0.XXs\n' in log
    assert '0.XXs → testing:retry() try=2\n' not in log


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


async def test_job_job_not_found_run_check(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('missing', _job_id='testing')
    worker: Worker = worker(functions=[foobar])
    with pytest.raises(FailedJobs) as exc_info:
        await worker.run_check()

    assert exc_info.value.count == 1
    assert len(exc_info.value.job_results) == 1
    failure = exc_info.value.job_results[0].result
    assert failure == JobExecutionFailed("function 'missing' not found")
    assert failure != 123  # check the __eq__ method of JobExecutionFailed


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


async def test_retry_lots_without_keep_result(arq_redis: ArqRedis, worker):
    async def retry(ctx):
        raise Retry()

    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')], keep_result=0)
    await worker.main()  # Should not raise MultiExecError


async def test_retry_lots_check(arq_redis: ArqRedis, worker, caplog):
    async def retry(ctx):
        raise Retry()

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')])
    with pytest.raises(FailedJobs, match='max 5 retries exceeded'):
        await worker.run_check()


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


async def test_job_expired_run_check(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    await arq_redis.delete(job_key_prefix + 'testing')
    worker: Worker = worker(functions=[foobar])
    with pytest.raises(FailedJobs) as exc_info:
        await worker.run_check()

    assert str(exc_info.value) in {
        "1 job failed JobExecutionFailed('job expired',)",  # python 3.6
        "1 job failed JobExecutionFailed('job expired')",  # python 3.7
    }
    assert exc_info.value.count == 1
    assert len(exc_info.value.job_results) == 1
    assert exc_info.value.job_results[0].result == JobExecutionFailed('job expired')


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
    assert 'error serializing result of testing:example' in log


async def test_log_health_check(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    worker: Worker = worker(functions=[foobar], health_check_interval=0)
    await worker.main()
    await worker.main()
    await worker.main()
    assert worker.jobs_complete == 1

    assert 'j_complete=1 j_failed=0 j_retried=0 j_ongoing=0 queued=0' in caplog.text
    # assert log.count('recording health') == 1 can happen more than once due to redis pool size
    assert 'recording health' in caplog.text


async def test_remain_keys(arq_redis: ArqRedis, worker):
    redis2 = await create_redis_pool(('localhost', 6379), encoding='utf8')
    try:
        await arq_redis.enqueue_job('foobar', _job_id='testing')
        assert sorted(await redis2.keys('*')) == ['arq:job:testing', 'arq:queue']
        worker: Worker = worker(functions=[foobar])
        await worker.main()
        assert sorted(await redis2.keys('*')) == ['arq:queue:health-check', 'arq:result:testing']
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
    assert sorted(await arq_redis.keys('*')) == ['arq:queue:health-check']


async def test_run_check_passes(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('foobar')
    await arq_redis.enqueue_job('foobar')
    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    assert 2 == await worker.run_check()


async def test_run_check_error(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('fails')
    worker: Worker = worker(functions=[func(fails, name='fails')])
    with pytest.raises(FailedJobs, match=r"1 job failed TypeError\('my type error'"):
        await worker.run_check()


async def test_run_check_error2(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('fails')
    await arq_redis.enqueue_job('fails')
    worker: Worker = worker(functions=[func(fails, name='fails')])
    with pytest.raises(FailedJobs, match='2 jobs failed:\n') as exc_info:
        await worker.run_check()
    assert len(exc_info.value.job_results) == 2


async def test_return_exception(arq_redis: ArqRedis, worker):
    async def return_error(ctx):
        return TypeError('xxx')

    j = await arq_redis.enqueue_job('return_error')
    worker: Worker = worker(functions=[func(return_error, name='return_error')])
    await worker.main()
    assert (worker.jobs_complete, worker.jobs_failed, worker.jobs_retried) == (1, 0, 0)
    r = await j.result(pole_delay=0)
    assert isinstance(r, TypeError)
    info = await j.result_info()
    assert info.success is True


async def test_error_success(arq_redis: ArqRedis, worker):
    j = await arq_redis.enqueue_job('fails')
    worker: Worker = worker(functions=[func(fails, name='fails')])
    await worker.main()
    assert (worker.jobs_complete, worker.jobs_failed, worker.jobs_retried) == (0, 1, 0)
    info = await j.result_info()
    assert info.success is False


async def test_many_jobs_expire(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foobar')
    await asyncio.gather(*[arq_redis.zadd(default_queue_name, 1, f'testing-{i}') for i in range(100)])
    worker: Worker = worker(functions=[foobar])
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 100
    assert worker.jobs_retried == 0

    log = '\n'.join(r.message for r in caplog.records)
    assert 'job testing-0 expired' in log
    assert log.count(' expired') == 100


async def test_repeat_job_result(arq_redis: ArqRedis, worker):
    j1 = await arq_redis.enqueue_job('foobar', _job_id='job_id')
    assert isinstance(j1, Job)
    assert await j1.status() == JobStatus.queued

    assert await arq_redis.enqueue_job('foobar', _job_id='job_id') is None

    await worker(functions=[foobar]).run_check()
    assert await j1.status() == JobStatus.complete

    assert await arq_redis.enqueue_job('foobar', _job_id='job_id') is None


async def test_queue_read_limit_equals_max_jobs(arq_redis: ArqRedis, worker):
    for _ in range(4):
        await arq_redis.enqueue_job('foobar')

    assert await arq_redis.zcard(default_queue_name) == 4
    worker: Worker = worker(functions=[foobar], max_jobs=2)
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    await worker._poll_iteration()
    await asyncio.sleep(0.1)
    assert await arq_redis.zcard(default_queue_name) == 2
    assert worker.jobs_complete == 2
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    await worker._poll_iteration()
    await asyncio.sleep(0.1)
    assert await arq_redis.zcard(default_queue_name) == 0
    assert worker.jobs_complete == 4
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0


async def test_custom_queue_read_limit(arq_redis: ArqRedis, worker):
    for _ in range(4):
        await arq_redis.enqueue_job('foobar')

    assert await arq_redis.zcard(default_queue_name) == 4
    worker: Worker = worker(functions=[foobar], max_jobs=4, queue_read_limit=2)
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    await worker._poll_iteration()
    await asyncio.sleep(0.1)
    assert await arq_redis.zcard(default_queue_name) == 2
    assert worker.jobs_complete == 2
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    await worker._poll_iteration()
    await asyncio.sleep(0.1)
    assert await arq_redis.zcard(default_queue_name) == 0
    assert worker.jobs_complete == 4
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0


async def test_custom_serializers(arq_redis_msgpack: ArqRedis, worker):
    j = await arq_redis_msgpack.enqueue_job('foobar', _job_id='job_id')
    worker: Worker = worker(
        functions=[foobar], job_serializer=msgpack.packb, job_deserializer=functools.partial(msgpack.unpackb, raw=False)
    )
    info = await j.info()
    assert info.function == 'foobar'
    assert await worker.run_check() == 1
    assert await j.result() == 42
    r = await j.info()
    assert r.result == 42


class UnpickleFails:
    def __init__(self, v):
        self.v = v

    def __setstate__(self, state):
        raise ValueError('this broke')


@pytest.mark.skipif(sys.version_info < (3, 7), reason='repr(exc) is ugly in 3.6')
async def test_deserialization_error(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('foobar', UnpickleFails('hello'), _job_id='job_id')
    worker: Worker = worker(functions=[foobar])
    with pytest.raises(FailedJobs) as exc_info:
        await worker.run_check()
    assert str(exc_info.value) == "1 job failed DeserializationError('unable to deserialize job')"


async def test_incompatible_serializers_1(arq_redis_msgpack: ArqRedis, worker):
    await arq_redis_msgpack.enqueue_job('foobar', _job_id='job_id')
    worker: Worker = worker(functions=[foobar])
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0


async def test_incompatible_serializers_2(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('foobar', _job_id='job_id')
    worker: Worker = worker(
        functions=[foobar], job_serializer=msgpack.packb, job_deserializer=functools.partial(msgpack.unpackb, raw=False)
    )
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0


async def test_max_jobs_completes(arq_redis: ArqRedis, worker):
    v = 0

    async def raise_second_time(ctx):
        nonlocal v
        v += 1
        if v > 1:
            raise ValueError('xxx')

    await arq_redis.enqueue_job('raise_second_time')
    await arq_redis.enqueue_job('raise_second_time')
    await arq_redis.enqueue_job('raise_second_time')
    worker: Worker = worker(functions=[func(raise_second_time, name='raise_second_time')])
    with pytest.raises(FailedJobs) as exc_info:
        await worker.run_check(max_burst_jobs=3)
    assert repr(exc_info.value).startswith('<2 jobs failed:')


async def test_max_bursts_sub_call(arq_redis: ArqRedis, worker, caplog):
    async def foo(ctx, v):
        return v + 1

    async def bar(ctx, v):
        await ctx['redis'].enqueue_job('foo', v + 1)

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('bar', 10)
    worker: Worker = worker(functions=[func(foo, name='foo'), func(bar, name='bar')])
    assert await worker.run_check(max_burst_jobs=1) == 1
    assert worker.jobs_complete == 1
    assert worker.jobs_retried == 0
    assert worker.jobs_failed == 0
    assert 'bar(10)' in caplog.text
    assert 'foo' in caplog.text


async def test_max_bursts_multiple(arq_redis: ArqRedis, worker, caplog):
    async def foo(ctx, v):
        return v + 1

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foo', 1)
    await arq_redis.enqueue_job('foo', 2)
    worker: Worker = worker(functions=[func(foo, name='foo')])
    assert await worker.run_check(max_burst_jobs=1) == 1
    assert worker.jobs_complete == 1
    assert worker.jobs_retried == 0
    assert worker.jobs_failed == 0
    assert 'foo(1)' in caplog.text
    assert 'foo(2)' not in caplog.text


async def test_max_bursts_dont_get(arq_redis: ArqRedis, worker):
    async def foo(ctx, v):
        return v + 1

    await arq_redis.enqueue_job('foo', 1)
    await arq_redis.enqueue_job('foo', 2)
    worker: Worker = worker(functions=[func(foo, name='foo')])

    worker.max_burst_jobs = 0
    assert len(worker.tasks) == 0
    await worker._poll_iteration()
    assert len(worker.tasks) == 0


async def test_non_burst(arq_redis: ArqRedis, worker, caplog, loop):
    async def foo(ctx, v):
        return v + 1

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foo', 1, _job_id='testing')
    worker: Worker = worker(functions=[func(foo, name='foo')])
    worker.burst = False
    t = loop.create_task(worker.main())
    await asyncio.sleep(0.1)
    t.cancel()
    assert worker.jobs_complete == 1
    assert worker.jobs_retried == 0
    assert worker.jobs_failed == 0
    assert '← testing:foo ● 2' in caplog.text


async def test_multi_exec(arq_redis: ArqRedis, worker, caplog):
    async def foo(ctx, v):
        return v + 1

    caplog.set_level(logging.DEBUG, logger='arq.worker')
    await arq_redis.enqueue_job('foo', 1, _job_id='testing')
    worker: Worker = worker(functions=[func(foo, name='foo')])
    await asyncio.gather(*[worker.run_jobs(['testing']) for _ in range(5)])
    # debug(caplog.text)
    assert 'multi-exec error, job testing already started elsewhere' in caplog.text
    assert 'WatchVariableError' not in caplog.text
