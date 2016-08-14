"""
:mod:`testing`
==============

pytest plugin and other utilities useful when writing tests for code using arq.

include the plugin in your tests's `conftest.py` file with::

    pytest_plugins = 'arq.testing'

See arq's own tests for examples of usage.
"""
import asyncio
import contextlib
import io
import logging
import os

import pytest
from aioredis import create_redis

from .utils import RedisMixin, timestamp
from .worker import BaseWorker

logger = logging.getLogger('arq.mock')


class RaiseWorker(BaseWorker):
    """
    Worker which raises exceptions rather than logging them. Useful for testing.
    """
    @classmethod
    def handle_execute_exc(cls, started_at, exc, j):
        raise exc

    def handle_prepare_exc(self, msg):
        raise RuntimeError(msg)


class MockRedis:
    """
    Very simple mock of aioredis > Redis which allows jobs to be enqueued and executed without
    redis.
    """
    def __init__(self, *, loop=None, data=None):
        self.loop = loop or asyncio.get_event_loop()
        self.data = {} if data is None else data
        logger.info('initialising MockRedis, data id: %s', None if data is None else id(data))

    async def rpush(self, list_name, data):
        logger.info('rpushing %s to %s', data, list_name)
        self.data[list_name] = self.data.get(list_name, []) + [data]

    async def blpop(self, *list_names, timeout=0):
        assert isinstance(timeout, int)
        start = timestamp() if timeout > 0 else None
        logger.info('blpop from %s, timeout=%d', list_names, timeout)
        while True:
            v = await self.lpop(*list_names)
            if v:
                return v
            t = timestamp() - start
            if start and t > timeout:
                logger.info('blpop timed out %0.3fs', t)
                return
            logger.info('blpop waiting for data %0.3fs', t)
            await asyncio.sleep(0.5, loop=self.loop)

    async def lpop(self, *list_names):
        for list_name in list_names:
            data_list = self.data.get(list_name)
            if data_list is None:
                continue
            assert isinstance(data_list, list)
            if data_list:
                d = data_list.pop(0)
                logger.info('lpop %s from %s', d, list_name)
                return list_name, d
        logger.info('lpop nothing found in lists %s', list_names)


class MockRedisPoolContextManager:
    def __init__(self, loop, data):
        self.loop = loop
        self.data = data

    async def __aenter__(self):
        return MockRedis(loop=self.loop, data=self.data)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class MockRedisPool:
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.data = {}

    def get(self):
        return MockRedisPoolContextManager(self.loop, self.data)

    def close(self):
        pass

    async def wait_closed(self):
        pass

    async def clear(self):
        self.data = {}


class MockRedisMixin(RedisMixin):
    """
    Dependent of RedisMixin which uses MockRedis rather than real redis to enqueue jobs.
    """
    async def create_redis_pool(self):
        return self._redis_pool or MockRedisPool(self.loop)

    @property
    def mock_data(self):
        self._redis_pool = self._redis_pool or MockRedisPool(self.loop)
        return self._redis_pool.data

    @mock_data.setter
    def mock_data(self, data):
        self._redis_pool = self._redis_pool or MockRedisPool(self.loop)
        self._redis_pool.data = data


class MockRedisWorker(MockRedisMixin, BaseWorker):
    """
    Dependent of Base Worker which executes jobs from MockRedis than real redis.
    """


@contextlib.contextmanager
def loop_context(existing_loop=None):
    """
    context manager which creates an asyncio loop.
    :param existing_loop: if supplied this loop is passed straight through and no new loop is created.
    """
    if existing_loop:
        # loop already exists, pass it straight through
        yield existing_loop
    else:
        _loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

        yield _loop

        _loop.stop()
        _loop.run_forever()
        _loop.close()
        asyncio.set_event_loop(None)


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
            testargs = {arg: pyfuncitem.funcargs[arg] for arg in pyfuncitem._fixtureinfo.argnames}

            task = _loop.create_task(pyfuncitem.obj(**testargs))
            _loop.run_until_complete(task)

        return True


@pytest.yield_fixture
def loop():
    """
    Yield fixture using loop_context()
    """
    with loop_context() as _loop:
        yield _loop


@pytest.yield_fixture
def tmpworkdir(tmpdir):
    """
    Create a temporary working working directory.
    """
    cwd = os.getcwd()
    os.chdir(tmpdir.strpath)

    yield tmpdir

    os.chdir(cwd)


@pytest.yield_fixture
def redis_conn(loop):
    """
    yield fixture which creates a redis connection, and flushes redis before the test.

    Note: redis is not flushed after the test both for performance and to allow later debugging.
    """
    async def _get_conn():
        conn = await create_redis(('localhost', 6379), loop=loop)
        await conn.flushall()
        return conn
    conn = loop.run_until_complete(_get_conn())
    conn.loop = loop
    yield conn

    conn.close()
    loop.run_until_complete(conn.wait_closed())


LOGS = ('arq.main', 'arq.work', 'arq.jobs')


class StreamLog:
    """
    Log stream object which allows one or more lots to be captured and tested.
    """
    def __init__(self):
        self.handler = None
        self.stream = io.StringIO()
        self.handler = logging.StreamHandler(stream=self.stream)
        self.loggers = []
        self.set_loggers()

    def set_loggers(self, *log_names, level=logging.INFO, fmt='%(name)s: %(message)s'):
        if self.loggers:
            self.finish()
        log_names = log_names or LOGS
        self.loggers = [logging.getLogger(log_name) for log_name in log_names]
        self.handler.setFormatter(logging.Formatter(fmt))
        for logger in self.loggers:
            logger.disabled = False
            logger.addHandler(self.handler)
        self.set_level(level)

    def set_level(self, level):
        for logger in self.loggers:
            logger.setLevel(level)

    def set_different_level(self, **levels):
        for log_name, level in levels.items():
            logger = logging.getLogger(log_name)
            logger.setLevel(level)

    @property
    def log(self):
        self.stream.seek(0)
        return self.stream.read()

    def finish(self):
        for logger in self.loggers:
            logger.removeHandler(self.handler)

    def __contains__(self, item):
        return item in self.log

    def __str__(self):
        return 'caplog:\n' + self.log

    def __repr__(self):
        return '< caplog: {!r}>'.format(self.log)


@pytest.yield_fixture
def caplog():
    """
    Similar to pytest's "capsys" except logs are captured not stdout and stderr

    See StreamLog for details on configuration and tests for examples of usage.
    """
    stream_log = StreamLog()

    yield stream_log

    stream_log.finish()


@pytest.yield_fixture
def debug():
    """
    fixture which causes all arq logs to display. For debugging purposes only, should alwasy
    be removed before committing.
    """
    # TODO: could be extended to also work as a context manager and allow more control.
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(asctime)s %(name)8s %(levelname)8s: %(message)s', datefmt='%H:%M:%S'))
    for logger_name in LOGS:
        l = logging.getLogger(logger_name)
        l.addHandler(handler)
        l.setLevel(logging.DEBUG)

    yield

    for logger_name in LOGS:
        l = logging.getLogger(logger_name)
        l.removeHandler(handler)
        l.setLevel(logging.NOTSET)
