import logging
import re
from asyncio import Future

import msgpack
import pytest

from arq import Actor, BaseWorker, concurrent
from arq.testing import MockRedisWorker as MockRedisBaseWorker

from .fixtures import (ChildActor, DemoActor, FoobarActor, MockRedisDemoActor, MockRedisWorker, ParentActor,
                       ParentChildActorWorker, Worker)


async def test_simple_job_dispatch(tmpworkdir, loop):
    actor = MockRedisDemoActor(loop=loop)
    assert None is await actor.add_numbers(1, 2)
    assert not tmpworkdir.join('add_numbers').exists()
    assert len(actor.mock_data) == 1
    assert list(actor.mock_data.keys())[0] == b'arq:q:dft'
    v = actor.mock_data[b'arq:q:dft']
    assert len(v) == 1
    data = msgpack.unpackb(v[0], raw=False)
    # timestamp
    assert 1e12 < data.pop(0) < 3e12
    assert data == ['MockRedisDemoActor', 'add_numbers', [1, 2], {}, '__id__']


async def test_concurrency_disabled_job_dispatch(tmpworkdir, loop):
    actor = MockRedisDemoActor(loop=loop, concurrency_enabled=False)
    assert None is await actor.add_numbers(1, 2)
    assert tmpworkdir.join('add_numbers').read_text('utf8') == '3'
    assert len(actor.mock_data) == 0


async def test_enqueue_redis_job(actor, redis_conn):
    assert not await redis_conn.exists(b'arq:q:dft')
    assert None is await actor.add_numbers(1, 2)

    assert await redis_conn.exists(b'arq:q:dft')
    dft_queue = await redis_conn.lrange(b'arq:q:dft', 0, -1)
    assert len(dft_queue) == 1
    data = msgpack.unpackb(dft_queue[0], raw=False)
    # timestamp
    assert 1e12 < data.pop(0) < 3e12
    assert data == ['DemoActor', 'add_numbers', [1, 2], {}, '__id__']


async def test_dispatch_work(tmpworkdir, loop, caplog, redis_conn):
    caplog.set_loggers(level=logging.DEBUG, fmt='%(message)s')
    actor = MockRedisDemoActor(loop=loop)
    assert None is await actor.add_numbers(1, 2)
    assert None is await actor.high_add_numbers(3, 4, c=5)
    assert len(actor.mock_data[b'arq:q:dft']) == 1
    assert len(actor.mock_data[b'arq:q:high']) == 1
    assert caplog.log == ('MockRedisDemoActor.add_numbers → dft\n'
                          'MockRedisDemoActor.high_add_numbers → high\n')
    worker = MockRedisWorker(burst=True, loop=actor.loop)
    worker.mock_data = actor.mock_data
    assert not tmpworkdir.join('add_numbers').exists()
    await worker.run()
    assert tmpworkdir.join('add_numbers').read() == '3'
    log = re.sub('0.0\d\ds', '0.0XXs', caplog.log)
    log = re.sub("arq:quit-.*", "arq:quit-<random>", log)
    log = re.sub(r'\d{4}-\d+-\d+ \d+:\d+:\d+', '<date time>', log)
    log = re.sub(r'\w{3}-\d+ \d+:\d+:\d+', '<date time2>', log)
    print(log)
    assert ('MockRedisDemoActor.add_numbers → dft\n'
            'MockRedisDemoActor.high_add_numbers → high\n'
            'Initialising work manager, burst mode: True, creating shadows...\n'
            'Using first shadows job class "JobConstID"\n'
            'Running worker with 1 shadow listening to 3 queues\n'
            'shadows: MockRedisDemoActor | queues: high, dft, low\n'
            'recording health: <date time2> j_complete=0 j_failed=0 j_timedout=0 j_ongoing=0 q_high=1 q_dft=1 q_low=0\n'
            'starting main blpop loop\n'
            'populating quit queue to prompt exit: arq:quit-<random>\n'
            'task semaphore locked: False\n'
            'yielding job, jobs in progress 1\n'
            'scheduling job <Job __id__ MockRedisDemoActor.high_add_numbers(3, 4, c=5) on high>, re-enqueue: False\n'
            'task semaphore locked: False\n'
            'yielding job, jobs in progress 2\n'
            'scheduling job <Job __id__ MockRedisDemoActor.add_numbers(1, 2) on dft>, re-enqueue: False\n'
            'task semaphore locked: False\n'
            'got job from the quit queue, stopping\n'
            'drain waiting 5.0s for 2 tasks to finish\n'
            'high queued  0.0XXs → __id__ MockRedisDemoActor.high_add_numbers(3, 4, c=5)\n'
            'high ran in  0.0XXs ← __id__ MockRedisDemoActor.high_add_numbers ● 12\n'
            'dft  queued  0.0XXs → __id__ MockRedisDemoActor.add_numbers(1, 2)\n'
            'dft  ran in  0.0XXs ← __id__ MockRedisDemoActor.add_numbers ● \n'
            'task complete, 1 jobs done, 0 failed\n'
            'task complete, 2 jobs done, 0 failed\n'
            'shutting down worker after 0.0XXs ◆ 2 jobs done ◆ 0 failed ◆ 0 timed out\n') == log


async def test_handle_exception(loop, caplog):
    caplog.set_loggers(fmt='%(message)s')
    actor = MockRedisDemoActor(loop=loop)
    assert caplog.log == ''
    assert None is await actor.boom()
    worker = MockRedisWorker(burst=True, loop=actor.loop)
    worker.mock_data = actor.mock_data
    await worker.run()
    log = re.sub('0.0\d\ds', '0.0XXs', caplog.log)
    log = re.sub(', line \d+,', ', line <no>,', log)
    log = re.sub('"/.*?/(\w+/\w+)\.py"', r'"/path/to/\1.py"', log)
    log = re.sub(r'\d{4}-\d+-\d+ \d+:\d+:\d+', '<date time>', log)
    log = re.sub(r'\w{3}-\d+ \d+:\d+:\d+', '<date time2>', log)
    print(log)
    assert ('Initialising work manager, burst mode: True, creating shadows...\n'
            'Running worker with 1 shadow listening to 3 queues\n'
            'shadows: MockRedisDemoActor | queues: high, dft, low\n'
            'recording health: <date time2> j_complete=0 j_failed=0 j_timedout=0 j_ongoing=0 q_high=0 q_dft=1 q_low=0\n'
            'drain waiting 5.0s for 1 tasks to finish\n'
            'dft  queued  0.0XXs → __id__ MockRedisDemoActor.boom()\n'
            'dft  ran in  0.0XXs ! __id__ MockRedisDemoActor.boom(): RuntimeError\n'
            'Traceback (most recent call last):\n'
            '  File "/path/to/arq/worker.py", line <no>, in run_job\n'
            '    result = await func(*j.args, **j.kwargs)\n'
            '  File "/path/to/arq/main.py", line <no>, in direct\n'
            '    return await self._func(self._self_obj, *args, **kwargs)\n'
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
            queues = ('a', 'a')
    assert excinfo.value.args[0] == "BadActor looks like it has duplicated queue names: ('a', 'a')"


async def test_custom_name(loop, caplog):
    actor = FoobarActor(loop=loop)
    assert re.match('^<FoobarActor\(foobar\) at 0x[a-f0-9]{12}>$', str(actor))
    assert None is await actor.concat('123', '456')
    worker = MockRedisWorker(burst=True, loop=actor.loop, shadows=[FoobarActor])
    worker.mock_data = actor.mock_data
    await worker.run()
    assert worker.jobs_failed == 0
    assert 'foobar.concat(123, 456)' in caplog.log


async def test_call_direct(mock_actor_worker, caplog):
    caplog.set_level(logging.INFO)
    actor, worker = mock_actor_worker
    await actor.enqueue_job('direct_method', 1, 2)
    await worker.run()
    assert worker.jobs_failed == 0
    assert worker.jobs_complete == 1
    log = re.sub('0.0\d\ds', '0.0XXs', caplog.log)
    assert ('arq.jobs: dft  queued  0.0XXs → __id__ MockRedisDemoActor.direct_method(1, 2)\n'
            'arq.jobs: dft  ran in  0.0XXs ← __id__ MockRedisDemoActor.direct_method ● 3') in log


async def test_direct_binding(mock_actor_worker, caplog):
    caplog.set_level(logging.INFO)
    actor, worker = mock_actor_worker
    assert None is await actor.concat('a', 'b')
    assert 'a + b' == await actor.concat.direct('a', 'b')
    await worker.run()
    assert worker.jobs_failed == 0
    assert worker.jobs_complete == 1
    assert 'MockRedisDemoActor.concat' in caplog
    assert 'MockRedisDemoActor.concat.direct' not in caplog


async def test_dynamic_worker(tmpworkdir, loop, redis_conn):
    actor = DemoActor(loop=loop)
    await actor.add_numbers(1, 2)
    assert not tmpworkdir.join('add_numbers').exists()
    worker = BaseWorker(loop=loop, burst=True, shadows=[DemoActor])
    await worker.run()
    await actor.close()
    assert tmpworkdir.join('add_numbers').exists()
    assert tmpworkdir.join('add_numbers').read() == '3'


async def test_dynamic_worker_mocked(tmpworkdir, loop):
    actor = MockRedisDemoActor(loop=loop)
    await actor.add_numbers(1, 2)
    assert not tmpworkdir.join('add_numbers').exists()
    worker = MockRedisBaseWorker(loop=loop, burst=True, shadows=[MockRedisDemoActor])
    worker.mock_data = actor.mock_data
    await worker.run()
    await actor.close()
    assert tmpworkdir.join('add_numbers').exists()
    assert tmpworkdir.join('add_numbers').read() == '3'


async def test_dynamic_worker_custom_queue(tmpworkdir, loop):
    class CustomActor(MockRedisDemoActor):
        queues = ['foobar']
    actor = CustomActor(loop=loop)
    await actor.enqueue_job('add_numbers', 1, 1, queue='foobar')
    assert not tmpworkdir.join('add_numbers').exists()
    worker = MockRedisBaseWorker(loop=loop, burst=True, queues=['foobar'], shadows=[CustomActor])
    worker.mock_data = actor.mock_data
    await worker.run()
    await actor.close()
    assert tmpworkdir.join('add_numbers').exists()
    assert tmpworkdir.join('add_numbers').read() == '2'


async def test_worker_no_shadow(loop):
    worker = MockRedisBaseWorker(loop=loop)
    with pytest.raises(TypeError) as excinfo:
        await worker.run()
    assert excinfo.value.args[0] == 'shadows not defined on worker'


async def test_mocked_actor(mocker, loop):
    m = mocker.patch('tests.test_main.DemoActor.direct_method')
    r = Future(loop=loop)
    r.set_result(123)
    m.return_value = r
    actor = DemoActor(loop=loop)
    v = await actor.direct_method(1, 1)
    assert v == 123


async def test_actor_wrapping(loop):
    actor = DemoActor(loop=loop)
    assert actor.add_numbers.__doc__ == 'add_number docs'
    assert actor.add_numbers.__name__ == 'add_numbers'
    assert repr(actor.add_numbers).startswith('<concurrent function DemoActor.add_numbers of <DemoActor(DemoActor) at')


async def test_bind_replication(tmpdir, loop):
    parent_actor = ParentActor(loop=loop)
    child_actor = ChildActor(loop=loop)
    child_actor.mock_data = parent_actor.mock_data
    file1 = tmpdir.join('test1')
    await parent_actor.save_value(str(file1))
    file2 = tmpdir.join('test2')
    await child_actor.save_value(str(file2))
    worker = ParentChildActorWorker(burst=True, loop=loop)
    worker.mock_data = parent_actor.mock_data
    await worker.run()
    assert file1.read() == 'Parent'
    assert file2.read() == 'Child'


async def test_encode_set(tmpworkdir, loop, redis_conn):
    actor = DemoActor(loop=loop)
    await actor.subtract({1, 2, 3, 4}, {4, 5})
    await actor.close()

    worker = Worker(loop=actor.loop, burst=True)
    await worker.run()
    assert worker.jobs_failed == 0
    assert tmpworkdir.join('subtract').exists()
    assert tmpworkdir.join('subtract').read() == '{1, 2, 3}'

    await worker.close()
