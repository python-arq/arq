import asyncio
import inspect
import logging
from functools import wraps

import msgpack

from .utils import *
from .worker import run_job


__all__ = [
    'Dispatch',
    'concurrent',
    'mode',
]

logger = logging.getLogger('arq.main')


class Mode:
    _redis = 'redis'
    _direct = 'direct'
    _asyncio_loop = 'asyncio_loop'
    _mode = _redis

    direct = property(lambda self: self._mode == self._direct)
    redis = property(lambda self: self._mode == self._redis)
    asyncio_loop = property(lambda self: self._mode == self._asyncio_loop)

    def set_redis(self):
        self._mode = self._redis

    def set_direct(self):
        self._mode = self._direct

    def set_asyncio_loop(self):
        self._mode = self._asyncio_loop

    def __str__(self):
        return self._mode

mode = Mode()


class Dispatch(RedisMixin):
    HIGH_QUEUE = b'high'
    DEFAULT_QUEUE = b'dft'
    LOW_QUEUE = b'low'
    __tasks = set()

    DEFAULT_QUEUES = (
        HIGH_QUEUE,
        DEFAULT_QUEUE,
        LOW_QUEUE
    )

    async def enqueue_job(self, func_name, *args, queue=None, **kwargs):
        queue = queue or self.DEFAULT_QUEUE
        class_name = self.__class__.__name__
        data = self.encode_args(
            queued_at=int(timestamp() * 1000),
            class_name=class_name,
            func_name=func_name,
            args=args,
            kwargs=kwargs,
        )
        logger.debug('%s.%s ▶ %s (mode: %s)', class_name, func_name, queue.decode(), mode)

        if mode.direct or mode.asyncio_loop:
            coro = run_job(queue, data, lambda j: self)
            if mode.direct:
                await coro
            else:
                self.__tasks.add(self.loop.create_task(coro))
        else:
            pool = await self.init_redis_pool()
            async with pool.get() as redis:
                await redis.rpush(queue, data)

    @staticmethod
    def encode_args(*, queued_at, class_name, func_name, args, kwargs):
        return msgpack.packb([queued_at, class_name, func_name, args, kwargs], use_bin_type=True)

    async def close(self):
        if mode.asyncio_loop:
            asyncio.wait(self.__tasks)
        await super().close()


def concurrent(func_or_queue):
    dec_queue = None

    def _func_wrapper(func):
        func_name = func.__name__

        if not inspect.iscoroutinefunction(func):
            raise TypeError('{} is not a coroutine function'.format(func.__qualname__))
        logger.debug('registering concurrent function %s', func.__qualname__)

        @wraps(func)
        async def _enqueuer(obj, *args, queue_name=None, **kwargs):
            await obj.enqueue_job(func_name, *args, queue=queue_name or dec_queue, **kwargs)

        _enqueuer.unbound_original = func
        return _enqueuer

    if isinstance(func_or_queue, str):
        func_or_queue = func_or_queue.encode()

    if isinstance(func_or_queue, bytes):
        dec_queue = func_or_queue
        return _func_wrapper
    else:
        return _func_wrapper(func_or_queue)
