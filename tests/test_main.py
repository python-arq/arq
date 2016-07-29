import asyncio
import os
import re

import pytest
import msgpack

from arq import arq_mode, Actor, concurrent

from .fixtures import Demo


async def test_simple_aloop(tmpworkdir, loop):
    arq_mode.set_asyncio_loop()
    assert str(arq_mode) == 'asyncio_loop'
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
    arq_mode.set_direct()
    demo = Demo(loop=loop)
    assert not os.path.exists('add_numbers')
    assert None is await demo.add_numbers(1, 2)
    assert os.path.exists('add_numbers')
    assert len(demo.arq_tasks) == 0
    with open('add_numbers') as f:
        assert f.read() == '3'


async def test_bad_def():
    with pytest.raises(TypeError) as excinfo:
        class BadActor(Actor):
            @concurrent
            def just_a_function(self):
                pass
    assert excinfo.value.args[0] == 'test_bad_def.<locals>.BadActor.just_a_function is not a coroutine function'


async def test_enqueue_redis_job(create_demo, redis_conn):
    demo = await create_demo()
    arq_mode.set_redis()
    conn = await redis_conn()
    assert not await conn.exists(b'arq:q:dft')
    assert None is await demo.add_numbers(1, 2)
    assert len(demo.arq_tasks) == 0

    assert await conn.exists(b'arq:q:dft')
    dft_queue = await conn.lrange(b'arq:q:dft', 0, -1)
    assert len(dft_queue) == 1
    data = msgpack.unpackb(dft_queue[0], encoding='utf8')
    # timestamp
    assert 1e10 < data.pop(0) < 2e10
    assert data == ['Demo', 'add_numbers', [1, 2], {}]

    assert os.path.exists('add_numbers') is False


async def test_logging(tmpworkdir, create_demo, logcap):
    demo = await create_demo()
    arq_mode.set_asyncio_loop()
    assert None is await demo.add_numbers(1, 2)
    assert None is await demo.high_add_numbers(3, 4, c=5)
    assert len(demo.arq_tasks) == 2
    assert not os.path.exists('add_numbers')
    assert logcap.log == ('Demo.add_numbers ▶ dft (mode: asyncio_loop)\n'
                          'Demo.high_add_numbers ▶ high (mode: asyncio_loop)\n')
    await asyncio.wait(demo.arq_tasks, loop=demo.loop)
    assert os.path.exists('add_numbers')
    log = re.sub('0.0\d\ds', '0.0XXs', logcap.log)
    assert log == ('Demo.add_numbers ▶ dft (mode: asyncio_loop)\n'
                   'Demo.high_add_numbers ▶ high (mode: asyncio_loop)\n'
                   'dft  queued  0.0XXs → Demo.add_numbers(1, 2)\n'
                   'dft  ran in  0.0XXs ← Demo.add_numbers ● \n'
                   'high queued  0.0XXs → Demo.high_add_numbers(3, 4, c=5)\n'
                   'high ran in  0.0XXs ← Demo.high_add_numbers ● 12\n')
