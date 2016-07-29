import logging
import os
import re

import pytest
import msgpack

from arq import Actor, concurrent

from .fixtures import MockRedisDemo, MockRedisWorker


async def test_simple_job_dispatch(loop):
    demo = MockRedisDemo(loop=loop)
    assert None is await demo.add_numbers(1, 2)
    assert len(demo.mock_data) == 1
    assert list(demo.mock_data.keys())[0] == b'arq:q:dft'
    v = demo.mock_data[b'arq:q:dft']
    assert len(v) == 1
    data = msgpack.unpackb(v[0], encoding='utf8')
    # timestamp
    assert 1e10 < data.pop(0) < 2e10
    assert data == ['MockRedisDemo', 'add_numbers', [1, 2], {}]


async def test_enqueue_redis_job(demo, redis_conn):
    conn = await redis_conn()
    assert not await conn.exists(b'arq:q:dft')
    assert None is await demo.add_numbers(1, 2)

    assert await conn.exists(b'arq:q:dft')
    dft_queue = await conn.lrange(b'arq:q:dft', 0, -1)
    assert len(dft_queue) == 1
    data = msgpack.unpackb(dft_queue[0], encoding='utf8')
    # timestamp
    assert 1e10 < data.pop(0) < 2e10
    assert data == ['Demo', 'add_numbers', [1, 2], {}]


async def test_dispatch_work(tmpworkdir, loop, logcap):
    logcap.set_level(logging.DEBUG)
    demo = MockRedisDemo(loop=loop)
    assert None is await demo.add_numbers(1, 2)
    assert None is await demo.high_add_numbers(3, 4, c=5)
    assert len(demo.mock_data[b'arq:q:dft']) == 1
    assert len(demo.mock_data[b'arq:q:high']) == 1
    assert logcap.log == ('MockRedisDemo.add_numbers ▶ dft\n'
                          'MockRedisDemo.high_add_numbers ▶ high\n')
    worker = MockRedisWorker(batch_mode=True, loop=demo.loop)
    worker.mock_data = demo.mock_data
    await worker.run()
    assert os.path.exists('add_numbers')
    log = re.sub('0.0\d\ds', '0.0XXs', logcap.log)
    assert log == ('MockRedisDemo.add_numbers ▶ dft\n'
                   'MockRedisDemo.high_add_numbers ▶ high\n'
                   'Initialising work manager, batch mode: True\n'
                   'Running worker with 1 shadow listening to 3 queues\n'
                   'shadows: MockRedisDemo, queues: high, dft, low\n'
                   'starting main blpop loop\n'
                   'scheduling job from queue high\n'
                   'scheduling job from queue dft\n'
                   'Quit msg, stopping work\n'
                   'waiting for 2 jobs to finish\n'
                   'high queued  0.0XXs → MockRedisDemo.high_add_numbers(3, 4, c=5)\n'
                   'high ran in  0.0XXs ← MockRedisDemo.high_add_numbers ● 12\n'
                   'dft  queued  0.0XXs → MockRedisDemo.add_numbers(1, 2)\n'
                   'dft  ran in  0.0XXs ← MockRedisDemo.add_numbers ● \n'
                   'shutting down worker after 0.0XXs, 2 jobs done\n')


async def test_handle_exception(loop, logcap):
    logcap.set_level(logging.INFO)
    demo = MockRedisDemo(loop=loop)
    assert logcap.log == ''
    assert None is await demo.boom()
    worker = MockRedisWorker(batch_mode=True, loop=demo.loop)
    worker.mock_data = demo.mock_data
    await worker.run()
    log = re.sub('0.0\d\ds', '0.0XXs', logcap.log)
    log = re.sub(', line \d+,', ', line <no>,', log)
    log = re.sub('"/.*?/(\w+/\w+)\.py"', r'"/path/to/\1.py"', log)

    assert log == ('Initialising work manager, batch mode: True\n'
                   'Running worker with 1 shadow listening to 3 queues\n'
                   'waiting for 1 jobs to finish\n'
                   'dft  queued  0.0XXs → MockRedisDemo.boom()\n'
                   'dft  ran in =  0.0XXs ! MockRedisDemo.boom: RuntimeError\n'
                   'Traceback (most recent call last):\n'
                   '  File "/path/to/arq/main.py", line <no>, in run_job\n'
                   '    result = await unbound_func(self, *j.args, **j.kwargs)\n'
                   '  File "/path/to/tests/fixtures.py", line <no>, in boom\n'
                   '    raise RuntimeError(\'boom\')\n'
                   'RuntimeError: boom\n'
                   'shutting down worker after 0.0XXs, 1 jobs done\n')


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
