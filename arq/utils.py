import asyncio
import base64
import os
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

import aioredis
from aioredis.pool import RedisPool

__all__ = [
    'ConnectionSettings',
    'RedisMixin',
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


def create_tz(seconds=0):
    if seconds == 0:
        return timezone.utc
    else:
        return timezone(timedelta(seconds=seconds))


EPOCH = datetime(1970, 1, 1)
EPOCH_TZ = EPOCH.replace(tzinfo=create_tz())


def timestamp():
    return (datetime.utcnow() - EPOCH).total_seconds()


def to_unix_timestamp(dt):
    utcoffset = dt.utcoffset()
    if utcoffset is not None:
        utcoffset = utcoffset.total_seconds()
        unix = (dt - EPOCH_TZ).total_seconds() + utcoffset
        return unix, int(utcoffset)
    else:
        return (dt - EPOCH).total_seconds(), None


def from_unix_timestamp(ts, utcoffset=None):
    dt = EPOCH + timedelta(seconds=ts)
    if utcoffset is not None:
        dt = dt.replace(tzinfo=create_tz(utcoffset))
    return dt


def gen_random(length=20):
    return base64.urlsafe_b64encode(os.urandom(length))[:length]


class cached_property:
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
