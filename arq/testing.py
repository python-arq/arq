"""
:mod:`testing`
==============

Utils for testing arq.

See arq's own tests for examples of usage.
"""
import asyncio
import logging

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
    Dependent of Base Worker which executes jobs from MockRedis rather than real redis.
    """
