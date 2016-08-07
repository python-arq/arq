import asyncio
import base64
import os
from collections import OrderedDict
from datetime import datetime

import aioredis
from aioredis.pool import RedisPool

__all__ = [
    'ConnectionSettings',
    'RedisMixin',
    'timestamp',
    'cached_property'
]


class SettingsMeta(type):
    __doc__ = 'Settings'
    __dict__ = None

    @classmethod
    def __prepare__(mcs, *args, **kwargs):
        return OrderedDict()

    def __new__(mcs, cls, bases, classdict):
        d = []
        for base in reversed(bases):
            if issubclass(base, ConnectionSettings):
                d.extend(base.__dict__.items())

        for k, v in classdict.items():
            if k[0] != '_' and k.upper() == k:
                d.append((k, v))
        classdict['__dict__'] = OrderedDict(d)
        return super().__new__(mcs, cls, bases, classdict)


class ConnectionSettings(metaclass=SettingsMeta):
    R_HOST = 'localhost'
    R_PORT = 6379
    R_DATABASE = 0
    R_PASSWORD = None

    def __init__(self, **custom_settings):
        for name, value in custom_settings.items():
            if not hasattr(self, name):
                raise TypeError('{} is not a valid setting name'.format(name))
            setattr(self, name, value)

    @property
    def dict(self):
        return self.__dict__

    def __iter__(self):
        yield from self.__dict__.items()


class RedisMixin:
    def __init__(self, *,
                 loop: asyncio.AbstractEventLoop=None,
                 settings: ConnectionSettings=None,
                 existing_pool: RedisPool=None):
        # the "or getattr(...) or" seems odd but it allows the mixin to work with subclasses with initialise
        # loop or settings before calling super().__init__ and don't pass those parameters.
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
