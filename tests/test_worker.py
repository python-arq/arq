import asyncio
import logging
from multiprocessing import Process
from unittest.mock import MagicMock

import pytest

import arq.worker
from arq.testing import RaiseWorker
from arq.worker import import_string, start_worker

from .example import ActorTest
from .fixtures import (EXAMPLE_FILE, DemoActor, FastShutdownWorker, FoobarActor, MockRedisDemoActor, MockRedisWorker,
                       MockRedisWorkerQuit, ReEnqueueActor, StartupActor, StartupWorker, Worker, WorkerFail, WorkerQuit,
                       kill_parent)


async def test_run_job_burst(tmpworkdir, redis_conn, actor):
    worker = Worker(burst=True, loop=actor.loop)

    await actor.add_numbers(1, 2)
    assert not tmpworkdir.join('add_numbers').exists()
    await worker.run()
    assert tmpworkdir.join('add_numbers').read() == '3'
    assert worker.jobs_failed == 0


async def test_long_args(mock_actor_worker, caplog):
    actor, worker = mock_actor_worker
    v = ','.join(map(str, range(20)))
    await actor.concat(a=v, b=v)
    await worker.run()
    log = caplog(('0.0\d\ds', '0.0XXs'))
    assert ("dft  queued  0.0XXs → MockRedisDemoActor.concat"
            "(a='0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19', b='0,1,2,3,4,5,6,7,8,9,1…)\n") in log
    assert ("dft  ran in  0.0XXs ← MockRedisDemoActor.concat ● "
            "'0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 + 0,1,2,3,4,5,6,7,8,9,10,11,…\n") in log


async def test_longer_args(mock_actor_worker, caplog):
    actor, worker = mock_actor_worker
    worker.log_curtail = 90
    v = ','.join(map(str, range(20)))
    await actor.concat(a=v, b=v)
    await worker.run()
    log = caplog(('0.0\d\ds', '0.0XXs'))
    assert ("dft  queued  0.0XXs → MockRedisDemoActor.concat"
            "(a='0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19', b='0,1,2,3,4,5,6,7,8,9,10,11,12,13…)\n") in log
    assert ("dft  ran in  0.0XXs ← MockRedisDemoActor.concat ● "
            "'0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 + 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,1…\n") in log


async def test_stop_job_normal(mock_actor_worker, caplog):
    caplog.set_loggers(fmt='%(name)s %(levelname)s: %(message)s')
    actor, worker = mock_actor_worker
    await actor.stop_job_normal()
    await worker.run()
    assert ('arq.jobs INFO: dft  ran in  0.0XXs ■ MockRedisDemoActor.stop_job_normal ● Stopped '
            'stopping job normally') in caplog(('0.0\d\ds', '0.0XXs'))


async def test_stop_job_warning(mock_actor_worker, caplog):
    caplog.set_loggers(fmt='%(name)s %(levelname)s: %(message)s')
    actor, worker = mock_actor_worker
    await actor.stop_job_warning()
    await worker.run()
    assert ('arq.jobs WARNING: dft  ran in  0.0XXs ■ MockRedisDemoActor.stop_job_warning ● Stopped Warning '
            'stopping job with warning') in caplog(('0.0\d\ds', '0.0XXs'))


async def test_separate_log_levels(mock_actor_worker, caplog):
    caplog.set_different_level(**{'arq.work': logging.INFO, 'arq.jobs': logging.WARNING})
    actor, worker = mock_actor_worker
    await actor.concat(a='1', b='2')
    await worker.run()
    log = caplog(('0.0\d\ds', '0.0XXs'))
    assert ('arq.work: Initialising work manager, burst mode: True\n'
            'arq.work: Running worker with 1 shadow listening to 3 queues\n'
            'arq.work: shadows: MockRedisDemoActor | queues: high, dft, low\n'
            'arq.work: drain waiting 5.0s for 1 tasks to finish\n'
            'arq.work: shutting down worker after 0.0XXs ◆ 1 jobs done ◆ 0 failed ◆ 0 timed out\n') == log


async def test_wrong_worker(mock_actor_worker, loop, caplog):
    actor, worker = mock_actor_worker
    actor2 = FoobarActor(loop=loop)
    actor2.mock_data = worker.mock_data
    assert None is await actor2.concat('a', 'b')
    await worker.run()
    assert worker.jobs_failed == 1
    assert 'Job Error: unable to find shadow for <Job foobar.concat(a, b) on dft>' in caplog


async def test_queue_not_found(loop):
    worker = MockRedisWorkerQuit(loop=loop, queues=['foobar'])
    with pytest.raises(KeyError) as excinfo:
        await worker.run()
    assert "queue not found in queue lookups from shadows, queues: ['foobar']" in excinfo.value.args[0]


async def test_mock_timeout(loop, caplog):
    caplog.set_loggers(log_names=('arq.main', 'arq.work', 'arq.mock'), level=logging.DEBUG)
    worker = MockRedisWorkerQuit(loop=loop)
    actor = MockRedisDemoActor(loop=loop)
    worker.mock_data = actor.mock_data

    assert None is await actor.concat('a', 'b')

    await worker.run()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert 'arq.mock: blpop timed out' in caplog


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


def test_import_start_worker(tmpworkdir, redis_conn, loop):
    actor = ActorTest(loop=loop)
    loop.run_until_complete(actor.foo(1, 2))

    assert loop.run_until_complete(redis_conn.exists(b'arq:q:dft'))
    dft_queue = loop.run_until_complete(redis_conn.lrange(b'arq:q:dft', 0, -1))
    assert len(dft_queue) == 1
    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    start_worker('test.py', 'Worker', True, loop=loop)
    assert tmpworkdir.join('foo').read() == '3'
    loop.run_until_complete(actor.close())


async def test_run_quit(tmpworkdir, redis_conn, actor, caplog):
    caplog.set_level(logging.DEBUG)

    await actor.save_slow(1, 0.1)
    await actor.save_slow(2, 0.1)
    await actor.save_slow(3, 0.1)
    await actor.save_slow(4, 0.1)
    assert 4 == await redis_conn.llen(b'arq:q:dft')

    assert not tmpworkdir.join('save_slow').exists()
    worker = WorkerQuit(loop=actor.loop)
    await worker.run()
    # the third job should be remove from the queue and readded
    assert tmpworkdir.join('save_slow').read() == '2'
    assert '1 pending tasks, waiting for one to finish' in caplog
    assert 2 == await redis_conn.llen(b'arq:q:dft')


async def test_task_exc(redis_conn, actor, caplog):
    caplog.set_level(logging.DEBUG)

    await actor.add_numbers(1, 2)
    await actor.close()

    worker = WorkerFail(loop=actor.loop)
    with pytest.raises(RuntimeError):
        await worker.run()
    assert 'Found task exception "foobar"' in caplog


def test_run_sigint(tmpworkdir, redis_conn, loop, caplog):
    caplog.set_level(logging.DEBUG)
    actor = ActorTest(loop=loop)

    loop.run_until_complete(actor.foo(1))
    loop.run_until_complete(actor.foo(1))
    loop.run_until_complete(actor.foo(1))
    loop.run_until_complete(actor.close())

    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    assert not tmpworkdir.join('foo').exists()
    start_worker('test.py', 'WorkerSignalQuit', False, loop=loop)
    assert tmpworkdir.join('foo').exists()
    assert tmpworkdir.join('foo').read() == '1'
    assert 'got signal: SIGINT, stopping...' in caplog


def test_run_sigint_twice(tmpworkdir, redis_conn, loop, caplog):
    caplog.set_level(logging.DEBUG)
    actor = ActorTest(loop=loop)

    loop.run_until_complete(actor.foo(1))
    loop.run_until_complete(actor.foo(1))
    loop.run_until_complete(actor.foo(1))
    loop.run_until_complete(actor.close())

    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    with pytest.raises(SystemExit):
        start_worker('test.py', 'WorkerSignalTwiceQuit', False, loop=loop)
    assert tmpworkdir.join('foo').exists()
    assert tmpworkdir.join('foo').read() == '1'
    assert 'Worker exiting after an unhandled error: ImmediateExit' in caplog


async def test_run_proxy_signal(actor, monkeypatch):
    mock_signal_signal = MagicMock()
    monkeypatch.setattr(arq.worker.signal, 'signal', mock_signal_signal)
    mock_signal_alarm = MagicMock()
    monkeypatch.setattr(arq.worker.signal, 'alarm', mock_signal_alarm)

    worker = Worker(burst=True, loop=actor.loop)
    assert worker.running is None
    assert mock_signal_signal.call_count == 3
    assert mock_signal_alarm.call_count == 0

    with pytest.raises(arq.worker.HandledExit):
        worker.handle_proxy_signal(arq.worker.SIG_PROXY, None)

    assert worker.running is None
    assert mock_signal_signal.call_count == 6
    assert mock_signal_alarm.call_count == 1


async def test_non_existent_function(redis_conn, actor, caplog):
    await actor.enqueue_job('doesnt_exist')
    worker = Worker(burst=True, loop=actor.loop)
    await worker.run()
    assert worker.jobs_failed == 1
    assert 'Job Error: shadow class "DemoActor" has no function "doesnt_exist"' in caplog


def test_no_jobs(loop):
    mock_actor = MockRedisDemoActor(loop=loop)
    mock_worker = MockRedisWorker(burst=True, loop=loop)
    mock_worker.mock_data = mock_actor.mock_data

    loop.run_until_complete(mock_actor.boom())
    loop.run_until_complete(mock_actor.concat('a', 'b'))
    mock_worker.run_until_complete()

    loop.run_until_complete(mock_worker.close())
    assert mock_worker.jobs_complete == 2
    assert mock_worker.jobs_failed == 1


async def test_shutdown_without_work(loop):
    mock_actor = MockRedisDemoActor(loop=loop)
    mock_worker = MockRedisWorker(loop=loop, burst=True)
    mock_worker.mock_data = mock_actor.mock_data
    await mock_worker.close()


async def test_job_timeout(loop, caplog):
    caplog.set_loggers()
    actor = MockRedisDemoActor(loop=loop)
    assert None is await actor.sleeper(0.2)
    assert None is await actor.sleeper(0.05)
    worker = MockRedisWorker(burst=True, loop=loop, timeout_seconds=0.1)
    worker.mock_data = actor.mock_data
    await worker.run()
    log = caplog(
        ('(\d.\d\d)\d', r'\1X'),
    )
    print(log)
    assert ('arq.jobs: dft  queued  0.00Xs → MockRedisDemoActor.sleeper(0.2)\n'
            'arq.jobs: dft  queued  0.00Xs → MockRedisDemoActor.sleeper(0.05)\n'
            'arq.jobs: dft  ran in  0.05Xs ← MockRedisDemoActor.sleeper ● 0.05\n'
            'arq.jobs: task timed out <Job MockRedisDemoActor.sleeper(0.2) on dft>\n'
            'arq.jobs: dft  ran in  0.10Xs ! MockRedisDemoActor.sleeper(0.2): CancelledError\n') in log
    assert ('concurrent.futures._base.CancelledError\n'
            'arq.work: shutting down worker after 0.10Xs ◆ 2 jobs done ◆ 1 failed ◆ 1 timed out\n') in log


def test_repeat_worker_close(tmpworkdir, redis_conn, caplog):
    tmpworkdir.join('test.py').write(EXAMPLE_FILE)
    loop = asyncio.new_event_loop()

    async def enqueue_jobs(_loop):
        actor = ActorTest(loop=_loop)
        for i in range(1, 6):
            await actor.foo(0, i)
        await actor.close()

    loop.run_until_complete(enqueue_jobs(loop))

    Process(target=kill_parent).start()
    start_worker('test.py', 'Worker', False, loop=loop)
    assert tmpworkdir.join('foo').exists()
    assert tmpworkdir.join('foo').read() == '5'  # because WorkerSignalQuit quit
    assert caplog.log.count('shutting down worker after') == 1


async def test_raise_worker_execute(redis_conn, actor):
    worker = RaiseWorker(burst=True, loop=actor.loop, shadows=[DemoActor])

    await actor.boom()
    with pytest.raises(RuntimeError) as excinfo:
        await worker.run()
    assert excinfo.value.args[0] == 'boom'
    await worker.close()


async def test_raise_worker_prepare(redis_conn, actor):
    worker = RaiseWorker(burst=True, loop=actor.loop, shadows=[DemoActor])

    await actor.enqueue_job('foobar', 1, 2)
    with pytest.raises(RuntimeError) as excinfo:
        await worker.run()
    assert excinfo.value.args[0] == 'Job Error: shadow class "DemoActor" has no function "foobar"'
    await worker.close()


async def test_reusable_worker(tmpworkdir, redis_conn, actor):
    worker = Worker(burst=True, loop=actor.loop)
    worker.reusable = True

    await actor.add_numbers(1, 2)
    assert not tmpworkdir.join('add_numbers').exists()
    await worker.run()
    assert tmpworkdir.join('add_numbers').read() == '3'
    assert worker.jobs_failed == 0

    await actor.add_numbers(3, 4)
    await worker.run()
    assert tmpworkdir.join('add_numbers').read() == '7'
    assert worker.jobs_failed == 0
    await worker.close()


async def test_startup_shutdown(tmpworkdir, redis_conn, loop):
    worker = StartupWorker(burst=True, loop=loop)

    actor = StartupActor(loop=loop)
    try:
        await actor.concurrent_func('foobar')
        assert not tmpworkdir.join('events').exists()
        await worker.run()
        assert tmpworkdir.join('events').read() == 'startup[True],concurrent_func[foobar],shutdown[True],'
        assert worker.jobs_failed == 0
    finally:
        await actor.close(True)
    assert tmpworkdir.join('events').read() == ('startup[True],concurrent_func[foobar],'
                                                'shutdown[True],shutdown[False],')


def test_check_successful(redis_conn, loop):
    loop.run_until_complete(redis_conn.set(b'arq:health-check', b'X'))
    assert 0 == Worker.check_health(loop=loop)


def test_check_fails(redis_conn, loop):
    assert 0 == loop.run_until_complete(redis_conn.exists(b'arq:health-check'))
    assert 1 == Worker.check_health(loop=loop)


async def test_check_successful_real_value(redis_conn, loop):
    assert 0 == await redis_conn.exists(b'arq:health-check')
    worker = Worker(burst=True, loop=loop)
    await worker.run()
    assert 1 == await redis_conn.exists(b'arq:health-check')
    assert 0 == await Worker(loop=loop)._check_health()


async def test_does_re_enqueue_job(loop, redis_conn):
    worker = FastShutdownWorker(burst=True, loop=loop, shadows=[ReEnqueueActor])

    actor = ReEnqueueActor(loop=loop)
    await actor.sleeper(0.2)
    await actor.close(True)

    await worker.run()
    assert 1 == await redis_conn.llen(b'arq:q:dft')


async def test_does_not_re_enqueue_job(loop, redis_conn):
    worker = FastShutdownWorker(burst=True, loop=loop, shadows=[DemoActor])

    actor = DemoActor(loop=loop)
    await actor.sleeper(0.2)
    await actor.close(True)

    await worker.run()
    assert 0 == await redis_conn.llen(b'arq:q:dft')
