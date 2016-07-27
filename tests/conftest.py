import asyncio
import contextlib
import gc
import logging
import os

import pytest


def _teardown_test_loop(_loop):
    is_closed = getattr(_loop, 'is_closed')
    if is_closed is not None:
        closed = is_closed()
    else:
        closed = _loop._closed
    if not closed:
        _loop.call_soon(_loop.stop)
        _loop.run_forever()
        _loop.close()
    gc.collect()
    asyncio.set_event_loop(None)


@contextlib.contextmanager
def loop_context(existing_loop=None):
    if existing_loop:
        # loop already exists, pass it straight through
        yield existing_loop
    else:
        _loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        yield _loop
        _teardown_test_loop(_loop)


def pytest_pycollect_makeitem(collector, name, obj):
    """
    Fix pytest collecting for coroutines.
    """
    if collector.funcnamefilter(name) and asyncio.iscoroutinefunction(obj):
        return list(collector._genfunctions(name, obj))


def pytest_pyfunc_call(pyfuncitem):
    """
    Run coroutines in an event loop instead of a normal function call.
    """
    if asyncio.iscoroutinefunction(pyfuncitem.function):
        existing_loop = pyfuncitem.funcargs.get('loop', None)
        with loop_context(existing_loop) as _loop:
            testargs = {arg: pyfuncitem.funcargs[arg]
                        for arg in pyfuncitem._fixtureinfo.argnames}

            task = _loop.create_task(pyfuncitem.obj(**testargs))
            _loop.run_until_complete(task)

        return True


@pytest.yield_fixture
def loop():
    with loop_context() as _loop:
        yield _loop


@pytest.yield_fixture
def debug_logger():
    handler = logging.StreamHandler()
    logger = logging.getLogger('.')
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    yield

    logger.removeHandler(handler)


@pytest.yield_fixture
def tmpworkdir(tmpdir):
    """
    Create a temporary working working directory.
    """
    cwd = os.getcwd()
    os.chdir(tmpdir.strpath)

    yield tmpdir

    os.chdir(cwd)
