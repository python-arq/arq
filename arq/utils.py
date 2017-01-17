"""
:mod:`utils`
============

Utilises for running arq used by other modules.
"""
import asyncio
import base64
import os
from datetime import datetime, timedelta, timezone
from typing import Tuple, Union

import aioredis
from aioredis.pool import RedisPool

__all__ = ['RedisSettings', 'RedisMixin']


class RedisSettings:
    """
    No-Op class used to hold redis connection redis_settings.
    """
    def __init__(self,
                 host='localhost',
                 port=6379,
                 database=0,
                 password=None):
        """
        :param host: redis host
        :param port: redis port
        :param database: redis database id
        :param password: password for redis connection
        """
        self.host = host
        self.port = port
        self.database = database
        self.password = password


class RedisMixin:
    """
    Mixin used to fined a redis pool and access it.
    """
    def __init__(self, *,
                 loop: asyncio.AbstractEventLoop=None,
                 redis_settings: RedisSettings=None,
                 existing_pool: RedisPool=None) -> None:
        """
        :param loop: asyncio loop to use for the redis pool
        :param redis_settings: connection settings to use for the pool
        :param existing_pool: existing pool, if set no new pool is created, instead this one is used
        """
        # the "or getattr(...) or" seems odd but it allows the mixin to work with subclasses which initialise
        # loop or redis_settings before calling super().__init__ and don't pass those parameters.
        self.loop = loop or getattr(self, 'loop', None) or asyncio.get_event_loop()
        self.redis_settings = redis_settings or getattr(self, 'redis_settings', None) or RedisSettings()
        self._redis_pool = existing_pool

    async def create_redis_pool(self) -> RedisPool:
        """
        Create a new redis pool.
        """
        return await aioredis.create_pool((self.redis_settings.host, self.redis_settings.port), loop=self.loop,
                                          db=self.redis_settings.database, password=self.redis_settings.password)

    async def get_redis_pool(self) -> RedisPool:
        """
        Get the redis pool, if a pool is already initialised it's returned, else one is crated.
        """
        if self._redis_pool is None:
            self._redis_pool = await self.create_redis_pool()
        return self._redis_pool

    async def get_redis_conn(self):
        """
        :return: redis connection context manager
        """
        pool = await self.get_redis_pool()
        return pool.get()

    async def close(self):
        """
        Close the pool and wait for all connections to close.
        """
        if self._redis_pool:
            self._redis_pool.close()
            await self._redis_pool.wait_closed()
            await self._redis_pool.clear()


def create_tz(utcoffset=0) -> timezone:
    """
    Create a python datetime.timezone with a given utc offset.

    :param utcoffset: utc offset in seconds, if 0 timezone.utc is returned.
    """
    if utcoffset == 0:
        return timezone.utc  # type: ignore
    else:
        return timezone(timedelta(seconds=utcoffset))


EPOCH = datetime(1970, 1, 1)
EPOCH_TZ = EPOCH.replace(tzinfo=create_tz())


def timestamp() -> float:
    """
    :return: now in unix time, eg. seconds since 1970
    """
    return (datetime.utcnow() - EPOCH).total_seconds()


def to_unix_ms(dt: datetime) -> Tuple[int, Union[int, None]]:
    """
    convert a datetime to number of milliseconds since 1970
    :param dt: datetime to evaluate
    :return: tuple - (unix time in milliseconds, utc offset in seconds)
    """
    utcoffset = dt.utcoffset()
    if utcoffset is not None:
        _utcoffset = utcoffset.total_seconds()
        unix = (dt - EPOCH_TZ).total_seconds() + _utcoffset
        return int(unix * 1000), int(_utcoffset)
    else:
        return int((dt - EPOCH).total_seconds() * 1000), None


def from_unix_ms(ms: int, utcoffset: int=None) -> datetime:
    """
    convert int to a datetime.

    :param ms: number of milliseconds since 1970
    :param utcoffset: if set a timezone i added to the datime based on the offset in seconds.
    :return: datetime - including timezone if utcoffset is not None, else timezone naïve
    """
    dt = EPOCH + timedelta(milliseconds=ms)
    if utcoffset is not None:
        dt = dt.replace(tzinfo=create_tz(utcoffset))
    return dt


def gen_random(length: int=20) -> bytes:
    """
    Create a random string.

    :param length: length of string to created, default 20
    """
    return base64.urlsafe_b64encode(os.urandom(length))[:length]


def ellipsis(s: str, length: int=80) -> str:
    """
    Truncate a string and add an ellipsis (three dots) to the end if it was too long

    :param s: string to possibly truncate
    :param length: length to truncate the string to
    """
    if len(s) > length:
        s = s[:length - 1] + '…'
    return s
