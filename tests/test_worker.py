import re
import logging

import pytest

from arq.worker import import_string, start_worker

from .fixtures import Worker, MockRedisTestActor, EXAMPLE_FILE, WorkerQuit, WorkerFail
from .example import ActorTest


async def test_run_job(tmpworkdir, redis_conn, actor):
    worker = Worker(batch_mode=True, loop=actor.loop)

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
    assert ('dft  queued  0.0XXs → MockRedisTestActor.concat'
            '(a=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19, b=0,1,2,3,4,5,6,7,8,9,10...)\n') in log
    assert ('dft  ran in  0.0XXs ← MockRedisTestActor.concat ● '
            '0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 + 0,1,2,3,4,5,6,7,8,9,10,11...\n') in log


async def test_wrong_worker(mock_actor_worker, logcap):
    actor, worker = mock_actor_worker
    actor2 = MockRedisTestActor(name='missing')
    actor2.mock_data = worker.mock_data
    assert None is await actor2.concat('a', 'b')
    await worker.run()
    assert worker.jobs_failed == 1
    assert 'Job Error: unable to find shadow for <Job missing.concat(a, b) on dft>' in logcap


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
    start_worker('test.py', 'WorkerSignalQuit', False)
    assert tmpworkdir.join('foo').exists()
    assert tmpworkdir.join('foo').read() == '1'
    assert 'got signal: SIGINT, stopping...' in logcap


async def test_non_existent_function(redis_conn, actor, logcap):
    await actor.enqueue_job('doesnt_exist')
    worker = Worker(batch_mode=True, loop=actor.loop)
    await worker.run()
    assert worker.jobs_failed == 1
    assert 'Job Error: shadow class "TestActor" has not function "doesnt_exist"' in logcap
