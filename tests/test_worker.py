import asyncio
import re
import logging

import pytest

from arq.worker import import_string, start_worker, ImmediateExit

from .fixtures import (Worker, EXAMPLE_FILE, WorkerQuit, WorkerFail, FoobarActor,
                       MockRedisWorkerQuit, MockRedisTestActor, MockRedisWorker)
from .example import ActorTest


async def test_run_job_batch(tmpworkdir, redis_conn, actor):
    worker = Worker(batch=True, loop=actor.loop)

    await actor.add_numbers(1, 2)
    assert not tmpworkdir.join('add_numbers').exists()
    await worker.run()
    assert tmpworkdir.join('add_numbers').read() == '3'
    assert worker.jobs_failed == 0


async def test_long_args(mock_actor_worker, logcap):
    actor, worker = mock_actor_worker
    v = ','.join(map(str, range(20)))
    await actor.concat(a=v, b=v)
    await worker.run()
    log = re.sub('0.0\d\ds', '0.0XXs', logcap.log)
    assert ("dft  queued  0.0XXs → MockRedisTestActor.concat"
            "(a='0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19', b='0,1,2,3,4,5,6,7,8,9,1…)\n") in log
    assert ("dft  ran in  0.0XXs ← MockRedisTestActor.concat ● "
            "'0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 + 0,1,2,3,4,5,6,7,8,9,10,11,…\n") in log


async def test_seperate_log_levels(mock_actor_worker, logcap):
    logcap.set_different_level(**{'arq.work': logging.INFO, 'arq.jobs': logging.WARNING})
    actor, worker = mock_actor_worker
    await actor.concat(a='1', b='2')
    await worker.run()
    log = re.sub('0.0\d\ds', '0.0XXs', logcap.log)
    assert ('arq.work: Initialising work manager, batch mode: True\n'
            'arq.work: Running worker with 1 shadow listening to 3 queues\n'
            'arq.work: shadows: MockRedisTestActor | queues: high, dft, low\n'
            'arq.work: shutting down worker, waiting for 1 jobs to finish\n'
            'arq.work: shutting down worker after 0.0XXs ◆ 1 jobs done ◆ 0 failed ◆ 0 timed out\n') == log

async def test_wrong_worker(mock_actor_worker, logcap):
    actor, worker = mock_actor_worker
    actor2 = FoobarActor()
    actor2.mock_data = worker.mock_data
    assert None is await actor2.concat('a', 'b')
    await worker.run()
    assert worker.jobs_failed == 1
    assert 'Job Error: unable to find shadow for <Job foobar.concat(a, b) on dft>' in logcap

async def test_queue_not_found(loop):
    class WrongWorker(MockRedisWorkerQuit):
        queues = ['foobar']

    worker = WrongWorker(loop=loop)
    with pytest.raises(KeyError) as excinfo:
        await worker.run()
    assert "queue not found in queue lookups from shadows, queues: ['foobar']" in excinfo.value.args[0]


async def test_mock_timeout(loop, logcap):
    logcap.set_loggers(('arq.main', 'arq.work', 'arq.mock'), logging.DEBUG)
    worker = MockRedisWorkerQuit(loop=loop)
    actor = MockRedisTestActor(loop=loop)
    worker.mock_data = actor.mock_data

    assert None is await actor.concat('a', 'b')

    await worker.run()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert 'arq.mock: blpop timed out' in logcap


def test_import_string_good(tmpworkdir):
    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    attr = import_string('test.py', 'Worker')
    assert attr.__name__ == 'Worker'
    assert attr.signature == 'foobar'


def test_import_string_missing_attr(tmpworkdir):
    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    with pytest.raises(ImportError):
        import_string('test.py', 'wrong')


def test_import_string_missing_file(tmpworkdir):
    with pytest.raises(ImportError):
        import_string('test.py', 'wrong')


async def test_import_start_worker(tmpworkdir, redis_conn, loop):
    actor = ActorTest(loop=loop)
    await actor.foo(1, 2)

    assert await redis_conn.exists(b'arq:q:dft')
    dft_queue = await redis_conn.lrange(b'arq:q:dft', 0, -1)
    assert len(dft_queue) == 1
    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    start_worker('test.py', 'Worker', True)
    assert tmpworkdir.join('foo').read() == '3'
    await actor.close()


async def test_run_quit(tmpworkdir, redis_conn, actor, logcap):
    logcap.set_level(logging.DEBUG)

    await actor.save_slow(1, 0.1)
    await actor.save_slow(2, 0.1)
    await actor.save_slow(3, 0.1)
    await actor.save_slow(4, 0.1)

    assert not tmpworkdir.join('save_slow').exists()
    worker = WorkerQuit(loop=actor.loop)
    await worker.run()
    assert tmpworkdir.join('save_slow').read() == '3'
    assert '1 pending tasks, waiting for one to finish before creating task for TestActor.save_slow(2, 0.1)' in logcap


async def test_task_exc(redis_conn, actor, logcap):
    logcap.set_level(logging.DEBUG)

    await actor.add_numbers(1, 2)
    await actor.close()

    worker = WorkerFail(loop=actor.loop)
    with pytest.raises(RuntimeError):
        await worker.run()
    assert 'Found task exception "foobar"' in logcap


async def test_run_sigint(tmpworkdir, redis_conn, loop, logcap):
    logcap.set_level(logging.DEBUG)
    actor = ActorTest(loop=loop)

    await actor.foo(1)
    await actor.foo(1)
    await actor.foo(1)
    await actor.close()

    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    assert not tmpworkdir.join('foo').exists()
    start_worker('test.py', 'WorkerSignalQuit', False)
    assert tmpworkdir.join('foo').exists()
    assert tmpworkdir.join('foo').read() == '1'
    assert 'got signal: SIGINT, stopping...' in logcap


async def test_run_sigint_twice(tmpworkdir, redis_conn, loop, logcap):
    logcap.set_level(logging.DEBUG)
    actor = ActorTest(loop=loop)

    await actor.foo(1)
    await actor.foo(1)
    await actor.foo(1)
    await actor.close()

    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    with pytest.raises(ImmediateExit):
        start_worker('test.py', 'WorkerSignalTwiceQuit', False)
    assert tmpworkdir.join('foo').exists()
    assert tmpworkdir.join('foo').read() == '1'
    assert 'Worker exiting after an unhandled error: ImmediateExit' in logcap


async def test_non_existent_function(redis_conn, actor, logcap):
    await actor.enqueue_job('doesnt_exist')
    worker = Worker(batch=True, loop=actor.loop)
    await worker.run()
    assert worker.jobs_failed == 1
    assert 'Job Error: shadow class "TestActor" has not function "doesnt_exist"' in logcap


def test_no_jobs():
    loop = asyncio.get_event_loop()
    mock_actor = MockRedisTestActor()
    mock_worker = MockRedisWorker(batch=True)
    mock_worker.mock_data = mock_actor.mock_data

    loop.run_until_complete(mock_actor.boom())
    loop.run_until_complete(mock_actor.concat('a', 'b'))
    mock_worker.run_until_complete()

    loop.run_until_complete(mock_worker.close())
    assert mock_worker.jobs_complete == 2
    assert mock_worker.jobs_failed == 1


async def test_shutdown_without_work(loop):
    mock_actor = MockRedisTestActor(loop=loop)
    mock_worker = MockRedisWorker(loop=loop, batch=True)
    mock_worker.mock_data = mock_actor.mock_data
    await mock_worker.close()


async def test_job_timeout(loop, logcap):
    logcap.set_loggers()
    actor = MockRedisTestActor(loop=loop)
    assert None is await actor.sleeper(0.2)
    assert None is await actor.sleeper(0.05)
    worker = MockRedisWorker(batch=True, loop=loop, timeout_seconds=0.1)
    worker.mock_data = actor.mock_data
    await worker.run()
    log = re.sub('(\d.\d\d)\d', r'\1X', logcap.log)
    log = re.sub(', line \d+,', ', line <no>,', log)
    log = re.sub('"/.*?/(\w+/\w+)\.py"', r'"/path/to/\1.py"', log)
    assert ('arq.jobs: dft  queued  0.00Xs → MockRedisTestActor.sleeper(0.2)\n'
            'arq.jobs: dft  queued  0.00Xs → MockRedisTestActor.sleeper(0.05)\n'
            'arq.jobs: dft  ran in  0.05Xs ← MockRedisTestActor.sleeper ● 0.05\n'
            'arq.jobs: job timed out <Job MockRedisTestActor.sleeper(0.2) on dft>\n'
            'arq.jobs: dft  ran in  0.10Xs ! MockRedisTestActor.sleeper(0.2): CancelledError\n') in log
    assert ('raise CancelledError\n'
            'concurrent.futures._base.CancelledError\n'
            'arq.work: shutting down worker after 0.10Xs ◆ 2 jobs done ◆ 1 failed ◆ 1 timed out\n') in log


# # FIXME not working until https://bitbucket.org/ned/coveragepy/issues/512
# import time
# import os
# import signal
# from multiprocessing import Process
# from .example import ActorTest
#
#
# def kill_parent():
#     time.sleep(0.5)
#     os.kill(os.getppid(), signal.SIGTERM)
#
#
# def test_repeat_worker_close(tmpworkdir, redis_conn, logcap):
#     tmpworkdir.join('test.py').write(EXAMPLE_FILE)
#     loop = asyncio.get_event_loop()
#
#     async def enqueue_jobs(_loop):
#         actor = ActorTest(loop=_loop)
#         for i in range(1, 6):
#             await actor.foo(0, i)
#         await actor.close()
#
#     loop.run_until_complete(enqueue_jobs(loop))
#
#     Process(target=kill_parent).start()
#     start_worker('test.py', 'Worker', False)
#     assert tmpworkdir.join('foo').exists()
#     assert tmpworkdir.join('foo').read() == '5'  # because WorkerSignalQuit quit
#     assert logcap.log.count('shutting down worker after') == 1
