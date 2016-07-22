import asyncio
from datetime import datetime

import aioredis


__all__ = [
    'RedisMixin',
    'timestamp',
    'cached_property'
]


class RedisMixin:
    redis_pool = None

    def __init__(self, *, loop=None, host='localhost', port=6379, **redis_kwargs):
        self.loop = loop or asyncio.get_event_loop()
        self._host = host
        self._port = port
        self._redis_kwargs = redis_kwargs

    async def init_redis_pool(self):
        if self.redis_pool is None:
            self.redis_pool = await aioredis.create_pool((self._host, self._port),
                                                         loop=self.loop, **self._redis_kwargs)
        return self.redis_pool

    def set_connection(self, redis_pool):
        self.redis_pool = redis_pool

    async def close(self):
        if self.redis_pool:
            await self.redis_pool.clear()


_EPOCH = datetime(2016, 1, 1)


def timestamp():
    return (datetime.now() - _EPOCH).total_seconds()


class cached_property(object):
    def __init__(self, func):
        self.__doc__ = getattr(func, '__doc__')
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value
