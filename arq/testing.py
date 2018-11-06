"""
:mod:`testing`
==============

Utils for testing arq.

See arq's own tests for examples of usage.
"""
import asyncio
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta

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


@contextmanager
def redis_context_manager(r):
    yield r


class MockRedis:
    """
    Very simple mock of aioredis > Redis which allows jobs to be enqueued and executed without
    redis.
    """
    def __init__(self, *, loop=None, data=None):
        self.loop = loop or asyncio.get_event_loop()
        self.data = {} if data is None else data
        self._expiry = {}
        self._pool_or_conn = type('MockConnection', (), {'_loop': self.loop})
        logger.info('initialising MockRedis, data id: %s', None if data is None else id(data))

    def __await__(self):
        return redis_context_manager(self)
        yield  # pragma: no cover

    async def rpush(self, list_name, data):
        logger.info('rpushing %s to %s', data, list_name)
        self.data[list_name] = self._get(list_name, []) + [data]

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
            data_list = self._get(list_name)
            if data_list is None:
                continue
            assert isinstance(data_list, list)
            if data_list:
                d = data_list.pop(0)
                logger.info('lpop %s from %s', d, list_name)
                return list_name, d
        logger.info('lpop nothing found in lists %s', list_names)

    def _get(self, key, default=None):
        expires = self._expiry.get(key, None)
        if expires and expires < datetime.now():
            return default
        return self.data.get(key, default)

    async def llen(self, list_name):
        return len(self._get(list_name, []))

    async def flushdb(self):
        self.data = {}

    async def set(self, key, value):
        self.data[key] = value

    async def setex(self, key, expires, value):
        self.data[key] = value
        self._expiry[key] = datetime.now() + timedelta(seconds=expires)

    async def expire(self, key, expires):
        self._expiry[key] = datetime.now() + timedelta(seconds=expires)

    async def get(self, key):
        return self._get(key)

    async def getset(self, key, value):
        old_value = self._get(key)
        await self.set(key, value)
        return old_value

    def close(self):
        pass

    async def wait_closed(self):
        pass


class MockRedisMixin(RedisMixin):
    """
    Dependent of RedisMixin which uses MockRedis rather than real redis to enqueue jobs.
    """

    async def create_redis_pool(self):
        return self.redis or MockRedis(loop=self.loop)

    @property
    def mock_data(self):
        self.redis = self.redis or MockRedis(loop=self.loop)
        return self.redis.data

    @mock_data.setter
    def mock_data(self, data):
        self.redis = self.redis or MockRedis(loop=self.loop)
        self.redis.data = data


class MockRedisWorker(MockRedisMixin, BaseWorker):
    """
    Dependent of Base Worker which executes jobs from MockRedis rather than real redis.
    """
