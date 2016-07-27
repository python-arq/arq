import pytest

from arq import mode, Dispatch, concurrent

from .fixtures import Demo


async def test_simple_aloop(tmpworkdir, loop):
    mode.set_asyncio_loop()
    demo = Demo(loop=loop)
    r1 = await demo.add_numbers(1, 2)
    assert r1 is None
    assert len(demo.arq_tasks) == 1
    coro = list(demo.arq_tasks)[0]
    # this is the run job coroutine
    r2 = await coro
    assert r2 is None
    with open('add_numbers') as f:
        assert f.read() == '3'
    await demo.close()


async def test_simple_direct(tmpworkdir, loop):
    mode.set_direct()
    demo = Demo(loop=loop)
    print(demo.arq_tasks)
    r1 = await demo.add_numbers(1, 2)
    assert r1 is None
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
