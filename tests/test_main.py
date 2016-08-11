import logging
import re

import msgpack
import pytest

from arq import Actor, concurrent, BaseWorker
from arq.testing import MockRedisWorker as MockRedisBaseWorker

from .fixtures import MockRedisTestActor, MockRedisWorker, FoobarActor, TestActor


async def test_simple_job_dispatch(loop, debug):
    actor = MockRedisTestActor(loop=loop)
    assert None is await actor.add_numbers(1, 2)
    assert len(actor.mock_data) == 1
    assert list(actor.mock_data.keys())[0] == b'arq:q:dft'
    v = actor.mock_data[b'arq:q:dft']
    assert len(v) == 1
    data = msgpack.unpackb(v[0], encoding='utf8')
    # timestamp
    assert 1e12 < data.pop(0) < 3e12
    assert data == ['MockRedisTestActor', 'add_numbers', [1, 2], {}]


async def test_enqueue_redis_job(actor, redis_conn):
    assert not await redis_conn.exists(b'arq:q:dft')
    assert None is await actor.add_numbers(1, 2)

    assert await redis_conn.exists(b'arq:q:dft')
    dft_queue = await redis_conn.lrange(b'arq:q:dft', 0, -1)
    assert len(dft_queue) == 1
    data = msgpack.unpackb(dft_queue[0], encoding='utf8')
    # timestamp
    assert 1e12 < data.pop(0) < 3e12
    assert data == ['TestActor', 'add_numbers', [1, 2], {}]


async def test_dispatch_work(tmpworkdir, loop, logcap, redis_conn):
    logcap.set_loggers(level=logging.DEBUG, fmt='%(message)s')
    actor = MockRedisTestActor(loop=loop)
    assert None is await actor.add_numbers(1, 2)
    assert None is await actor.high_add_numbers(3, 4, c=5)
    assert len(actor.mock_data[b'arq:q:dft']) == 1
    assert len(actor.mock_data[b'arq:q:high']) == 1
    assert logcap.log == ('MockRedisTestActor.add_numbers ▶ dft\n'
                          'MockRedisTestActor.high_add_numbers ▶ high\n')
    worker = MockRedisWorker(batch=True, loop=actor.loop)
    worker.mock_data = actor.mock_data
    assert not tmpworkdir.join('add_numbers').exists()
    await worker.run()
    assert tmpworkdir.join('add_numbers').read() == '3'
    log = re.sub('0.0\d\ds', '0.0XXs', logcap.log)
    log = re.sub("QUIT-.*", "QUIT-<random>", log)
    assert ('MockRedisTestActor.add_numbers ▶ dft\n'
            'MockRedisTestActor.high_add_numbers ▶ high\n'
            'Initialising work manager, batch mode: True\n'
            'Running worker with 1 shadow listening to 3 queues\n'
            'shadows: MockRedisTestActor | queues: high, dft, low\n'
            'populating quit queue to prompt exit: QUIT-<random>\n'
            'starting main blpop loop\n'
            'scheduling job from queue high\n'
            'scheduling job from queue dft\n'
            'got job from the quit queue, stopping\n'
            'shutting down worker, waiting for 2 jobs to finish\n'
            'high queued  0.0XXs → MockRedisTestActor.high_add_numbers(3, 4, c=5)\n'
            'high ran in  0.0XXs ← MockRedisTestActor.high_add_numbers ● 12\n'
            'dft  queued  0.0XXs → MockRedisTestActor.add_numbers(1, 2)\n'
            'dft  ran in  0.0XXs ← MockRedisTestActor.add_numbers ● \n'
            'task complete, 1 jobs done, 0 failed\n'
            'task complete, 2 jobs done, 0 failed\n'
            'shutting down worker after 0.0XXs ◆ 2 jobs done ◆ 0 failed ◆ 0 timed out\n') == log
    # quick check of logcap's str and repr
    assert str(logcap).startswith('logcap:\nMockRedisTestActor')
    assert repr(logcap).startswith("< logcap: 'MockRedisTestActor")


async def test_handle_exception(loop, logcap):
    logcap.set_loggers(fmt='%(message)s')
    actor = MockRedisTestActor(loop=loop)
    assert logcap.log == ''
    assert None is await actor.boom()
    worker = MockRedisWorker(batch=True, loop=actor.loop)
    worker.mock_data = actor.mock_data
    await worker.run()
    log = re.sub('0.0\d\ds', '0.0XXs', logcap.log)
    log = re.sub(', line \d+,', ', line <no>,', log)
    log = re.sub('"/.*?/(\w+/\w+)\.py"', r'"/path/to/\1.py"', log)
    assert ('Initialising work manager, batch mode: True\n'
            'Running worker with 1 shadow listening to 3 queues\n'
            'shadows: MockRedisTestActor | queues: high, dft, low\n'
            'shutting down worker, waiting for 1 jobs to finish\n'
            'dft  queued  0.0XXs → MockRedisTestActor.boom()\n'
            'dft  ran in  0.0XXs ! MockRedisTestActor.boom(): RuntimeError\n'
            'Traceback (most recent call last):\n'
            '  File "/path/to/arq/worker.py", line <no>, in run_job\n'
            '    result = await func(*j.args, **j.kwargs)\n'
            '  File "/path/to/tests/fixtures.py", line <no>, in boom\n'
            '    raise RuntimeError(\'boom\')\n'
            'RuntimeError: boom\n'
            'shutting down worker after 0.0XXs ◆ 1 jobs done ◆ 1 failed ◆ 0 timed out\n') == log


async def test_bad_def():
    with pytest.raises(TypeError) as excinfo:
        class BadActor(Actor):
            @concurrent
            def just_a_function(self):
                pass
    assert excinfo.value.args[0] == 'test_bad_def.<locals>.BadActor.just_a_function is not a coroutine function'


async def test_repeat_queue():
    with pytest.raises(AssertionError) as excinfo:
        class BadActor(Actor):
            QUEUES = ('a', 'a')
    assert excinfo.value.args[0] == "BadActor looks like it has duplicated queue names: ('a', 'a')"


async def test_duplicate_direct_name():
    class BadActor(Actor):
        @concurrent
        async def foo(self):
            pass

        def foo_direct(self):
            pass
    with pytest.raises(RuntimeError) as excinfo:
        BadActor()
    print(excinfo.value)
    assert excinfo.value.args[0] == ('BadActor already has a method "foo_direct", '
                                     'this breaks arq direct method binding of "foo"')


async def test_custom_name(loop, logcap):
    actor = FoobarActor(loop=loop)
    assert re.match('^<FoobarActor\(foobar\) at 0x[a-f0-9]{12}>$', str(actor))
    assert None is await actor.concat('123', '456')
    worker = MockRedisWorker(batch=True, loop=actor.loop, shadows=[FoobarActor])
    worker.mock_data = actor.mock_data
    await worker.run()
    assert worker.jobs_failed == 0
    assert 'foobar.concat(123, 456)' in logcap.log


async def test_call_direct(mock_actor_worker, logcap):
    logcap.set_level(logging.INFO)
    actor, worker = mock_actor_worker
    await actor.enqueue_job('direct_method', 1, 2)
    await worker.run()
    assert worker.jobs_failed == 0
    assert worker.jobs_complete == 1
    log = re.sub('0.0\d\ds', '0.0XXs', logcap.log)
    assert ('arq.jobs: dft  queued  0.0XXs → MockRedisTestActor.direct_method(1, 2)\n'
            'arq.jobs: dft  ran in  0.0XXs ← MockRedisTestActor.direct_method ● 3') in log


async def test_direct_binding(mock_actor_worker, logcap):
    logcap.set_level(logging.INFO)
    actor, worker = mock_actor_worker
    assert None is await actor.concat('a', 'b')
    assert 'a + b' == await actor.concat_direct('a', 'b')
    await worker.run()
    assert worker.jobs_failed == 0
    assert worker.jobs_complete == 1
    assert 'MockRedisTestActor.concat' in logcap
    assert 'MockRedisTestActor.concat_direct' not in logcap


async def test_dynamic_worker(tmpworkdir, loop, redis_conn):
    actor = TestActor(loop=loop)
    await actor.add_numbers(1, 2)
    assert not tmpworkdir.join('add_numbers').exists()
    worker = BaseWorker(loop=loop, batch=True, shadows=[TestActor])
    await worker.run()
    await actor.close()
    assert tmpworkdir.join('add_numbers').exists()
    assert tmpworkdir.join('add_numbers').read() == '3'


async def test_dynamic_worker_mocked(tmpworkdir, loop):
    actor = MockRedisTestActor(loop=loop)
    await actor.add_numbers(1, 2)
    assert not tmpworkdir.join('add_numbers').exists()
    worker = MockRedisBaseWorker(loop=loop, batch=True, shadows=[MockRedisTestActor])
    worker.mock_data = actor.mock_data
    await worker.run()
    await actor.close()
    assert tmpworkdir.join('add_numbers').exists()
    assert tmpworkdir.join('add_numbers').read() == '3'


async def test_dynamic_worker_custom_queue(tmpworkdir, loop):
    class CustomActor(MockRedisTestActor):
        QUEUES = ['foobar']
    actor = CustomActor(loop=loop)
    await actor.enqueue_job('add_numbers', 1, 1, queue='foobar')
    assert not tmpworkdir.join('add_numbers').exists()
    worker = MockRedisBaseWorker(loop=loop, batch=True, queues=['foobar'], shadows=[CustomActor])
    worker.mock_data = actor.mock_data
    await worker.run()
    await actor.close()
    assert tmpworkdir.join('add_numbers').exists()
    assert tmpworkdir.join('add_numbers').read() == '2'


def test_worker_no_shadow():
    with pytest.raises(TypeError) as excinfo:
        MockRedisBaseWorker()
    assert excinfo.value.args[0] == 'shadows not defined on worker'
