import asyncio
import contextlib
import io
import logging
import os

import pytest
from aioredis import create_redis

from arq import BaseWorker, RedisMixin, timestamp

logger = logging.getLogger('arq.mock')


class MockRedis:
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
    pass


@contextlib.contextmanager
def loop_context(existing_loop=None):
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
    def __init__(self):
        self.handler = None
        self.stream = io.StringIO()
        self.handler = logging.StreamHandler(stream=self.stream)
        self.loggers = []
        self.set_loggers()

    def set_loggers(self, log_names=LOGS, level=logging.INFO, fmt='%(name)s: %(message)s'):
        if self.loggers:
            self.finish()
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
        return 'logcap:\n' + self.log

    def __repr__(self):
        return '< logcap: {!r}>'.format(self.log)


@pytest.yield_fixture
def logcap():
    stream_log = StreamLog()

    yield stream_log

    stream_log.finish()


@pytest.yield_fixture
def debug(logs=LOGS, level=logging.DEBUG):
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter('%(asctime)s %(name)8s %(levelname)8s: %(message)s', datefmt='%H:%M:%S'))
    for logger_name in logs:
        logger = logging.getLogger(logger_name)
        logger.addHandler(handler)
        logger.setLevel(level)

    yield

    for logger_name in logs:
        logger = logging.getLogger(logger_name)
        logger.removeHandler(handler)
        logger.setLevel(logging.NOTSET)
