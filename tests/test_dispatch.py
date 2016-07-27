import asyncio
import os

import pytest
import msgpack

from arq import mode, Dispatch, concurrent

from .fixtures import Demo


async def test_simple_aloop(tmpworkdir, loop):
    mode.set_asyncio_loop()
    demo = Demo(loop=loop)
    assert None is await demo.add_numbers(1, 2)
    assert len(demo.arq_tasks) == 1
    assert not os.path.exists('add_numbers')
    await asyncio.wait(demo.arq_tasks, loop=loop)
    assert os.path.exists('add_numbers')

    with open('add_numbers') as f:
        assert f.read() == '3'
    await demo.close()


async def test_simple_direct(tmpworkdir, loop):
    mode.set_direct()
    demo = Demo(loop=loop)
    assert not os.path.exists('add_numbers')
    assert None is await demo.add_numbers(1, 2)
    assert os.path.exists('add_numbers')
    assert len(demo.arq_tasks) == 0
    with open('add_numbers') as f:
        assert f.read() == '3'


async def test_bad_def():
    with pytest.raises(TypeError) as excinfo:
        class BadDispatch(Dispatch):
            @concurrent
            def just_a_function(self):
                pass
    assert excinfo.value.args[0] == 'test_bad_def.<locals>.BadDispatch.just_a_function is not a coroutine function'


async def test_enqueue_redis_job(loop, redis_conn):
    mode.set_redis()
    conn = await redis_conn()
    assert not await conn.exists(b'arq-dft')
    demo = Demo(loop=loop)
    assert None is await demo.add_numbers(1, 2)
    assert len(demo.arq_tasks) == 0

    assert await conn.exists(b'arq-dft')
    dft_queue = await conn.lrange(b'arq-dft', 0, -1)
    assert len(dft_queue) == 1
    data = msgpack.unpackb(dft_queue[0], encoding='utf8')
    # timestamp
    assert 1e10 < data.pop(0) < 2e10
    assert data == ['Demo', 'add_numbers', [1, 2], {}]

    assert os.path.exists('add_numbers') is False
    await demo.close()
