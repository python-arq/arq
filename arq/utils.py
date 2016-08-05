import asyncio
import base64
import os
from datetime import datetime

import aioredis

__all__ = [
    'ConnectionSettings',
    'RedisMixin',
    'timestamp',
    'cached_property'
]


class ConnectionSettings:
    R_HOST = 'localhost'
    R_PORT = 6379
    R_DATABASE = 0
    R_PASSWORD = None

    def __init__(self, **custom_settings):
        for name, value in custom_settings.items():
            if not hasattr(self, name):
                raise TypeError('{} is not a valid setting name'.format(name))
            setattr(self, name, value)


class RedisMixin:
    def __init__(self, *, loop=None, settings: ConnectionSettings=None, existing_pool=None):
        self.loop = loop or getattr(self, 'loop', None) or asyncio.get_event_loop()
        self._settings = settings or getattr(self, '_settings', None) or ConnectionSettings()
        self._redis_pool = existing_pool

    async def create_redis_pool(self):
        return await aioredis.create_pool((self._settings.R_HOST, self._settings.R_PORT), loop=self.loop,
                                          db=self._settings.R_DATABASE, password=self._settings.R_PASSWORD)

    async def get_redis_pool(self):
        if self._redis_pool is None:
            self._redis_pool = await self.create_redis_pool()
        return self._redis_pool

    async def get_redis_conn(self):
        pool = await self.get_redis_pool()
        return pool.get()

    async def close(self):
        if self._redis_pool:
            self._redis_pool.close()
            await self._redis_pool.wait_closed()
            await self._redis_pool.clear()


_EPOCH = datetime(2016, 1, 1)


def timestamp():
    return (datetime.now() - _EPOCH).total_seconds()


def gen_random(length=20):
    return base64.urlsafe_b64encode(os.urandom(length))[:length]


class cached_property(object):
    def __init__(self, func):
        self.__doc__ = getattr(func, '__doc__')
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value


def ellipsis(s, length=80):
    if len(s) > length:
        s = s[:length - 1] + '…'
    return s
