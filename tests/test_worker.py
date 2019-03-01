import asyncio
import logging
import re
import signal
from unittest.mock import MagicMock

from arq.constants import health_check_key
from arq.worker import RetryJob, Worker, acheck_health, check_health, func, run_worker


async def foobar(ctx):
    return 42


def test_no_jobs(arq_redis, loop):
    class Settings:
        functions = [func(foobar, name='foobar')]
        burst = True
        poll_delay = 0

    loop.run_until_complete(arq_redis.enqueue_job('foobar'))
    worker = run_worker(Settings)
    assert worker.jobs_complete == 1
    assert str(worker) == '<Worker j_complete=1 j_failed=0 j_retried=0 j_ongoing=0>'


def test_health_check_fails(loop):
    class Settings:
        pass

    assert check_health(Settings) == 1


async def test_health_check_pass(redis):
    await redis.set(health_check_key, b'1')
    assert 0 == await acheck_health(None)


async def test_handle_sig(caplog):
    caplog.set_level(logging.INFO)
    worker = Worker([foobar])
    worker.main_task = MagicMock()
    worker.tasks = [MagicMock()]

    assert len(caplog.records) == 0
    worker.handle_sig(signal.SIGINT)
    assert len(caplog.records) == 1
    assert caplog.records[0].message == (
        'shutdown on SIGINT ◆ 0 jobs complete ◆ 0 failed ◆ 0 retries ◆ 1 ongoing to cancel'
    )
    assert worker.main_task.cancel.call_count == 1
    assert worker.tasks[0].done.call_count == 1
    assert worker.tasks[0].cancel.call_count == 0


async def test_job_successful(arq_redis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    worker: Worker = worker(functions=[foobar])
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.arun()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    log = re.sub(r'(\d+.\d\d)s', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert ('  X.XXs → testing.foobar()\n' '  X.XXs ← testing:foobar ● 42') in log


async def test_job_retry(arq_redis, worker, caplog):
    async def retry(ctx):
        if ctx['job_try'] <= 2:
            raise RetryJob(defer=0.1)

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')])
    await worker.arun()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 2

    log = re.sub(r'(\d+.\d\d)s', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'X.XXs ↻ testing:retry retrying job in X.XXs\n' in log
    assert 'X.XXs → testing.retry() try=2\n' in log
    assert 'X.XXs ← testing:retry ●' in log


async def test_job_job_not_found(arq_redis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('missing', _job_id='testing')
    worker: Worker = worker(functions=[foobar])
    await worker.arun()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0

    log = re.sub(r'(\d+.\d\d)s', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert "job testing, function 'missing' not found" in log


async def test_retry_lots(arq_redis, worker, caplog):
    async def retry(ctx):
        raise RetryJob()

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')])
    await worker.arun()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 5

    log = re.sub(r'(\d+.\d\d)s', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert '  X.XXs ! testing:retry max retries 5 exceeded' in log


async def test_cancel_error(arq_redis, worker, caplog):
    async def retry(ctx):
        if ctx['job_try'] == 1:
            raise asyncio.CancelledError()

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')])
    await worker.arun()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 1

    log = re.sub(r'(\d+.\d\d)s', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'X.XXs ↻ testing:retry cancelled, will be run again' in log
