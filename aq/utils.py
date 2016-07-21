import asyncio
import logging
from importlib import import_module

import aioredis


__all__ = [
    'aq_logger',
    'AQError',
    'RedisMixin',
    'import_string'
]

aq_logger = logging.getLogger('aq')


class AQError(RuntimeError):
    pass


class RedisMixin:
    redis_pool = None

    def __init__(self, *, loop=None, host='localhost', port=6379, **redis_kwargs):
        self._loop = loop or asyncio.get_event_loop()
        self._host = host
        self._port = port
        self._redis_kwargs = redis_kwargs

    async def init_redis_pool(self):
        if self.redis_pool is None:
            self.redis_pool = await aioredis.create_pool((self._host, self._port),
                                                         loop=self._loop, **self._redis_kwargs)
        return self.redis_pool

    def set_connection(self, redis_pool):
        self.redis_pool = redis_pool

    async def clear(self):
        if self.redis_pool:
            await self.redis_pool.clear()


def import_string(dotted_path):
    """
    Import a dotted module path and return the attribute/class designated by the
    last name in the path. Raise ImportError if the import failed.
    """
    try:
        module_path, class_name = dotted_path.rsplit('.', 1)
    except ValueError as e:
        raise ImportError("%s doesn't look like a module path" % dotted_path) from e

    module = import_module(module_path)

    try:
        return getattr(module, class_name)
    except AttributeError as e:
        raise ImportError('Module "%s" does not define a "%s" attribute/class' % (module_path, class_name)) from e
