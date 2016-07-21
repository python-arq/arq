import inspect
from functools import wraps

import msgpack

from .utils import *

__all__ = [
    'Dispatch',
    'concurrent',
    'aq_control'
]


class Control:
    concurrent = True

aq_control = Control()


class Dispatch(RedisMixin):
    HIGH_QUEUE = b'high'
    DEFAULT_QUEUE = b'dft'
    LOW_QUEUE = b'low'

    DEFAULT_QUEUES = (
        HIGH_QUEUE,
        DEFAULT_QUEUE,
        LOW_QUEUE
    )

    async def enqueue_job(self, func_name, *args, queue_name=None, **kwargs):
        queue_name = queue_name or self.DEFAULT_QUEUE

        if not aq_control.concurrent:
            func = getattr(self, func_name)
            unbound_original = getattr(func, 'unbound_original', None)
            if unbound_original:
                await unbound_original(self, *args, **kwargs)
            else:
                await func(*args, **kwargs)
        else:
            pool = await self.init_redis_pool()
            class_name = self.__class__.__name__
            aq_logger.debug('%s.%s ▶ %s', class_name, func_name, queue_name.decode())

            data = self.encode_args(class_name, func_name, args, kwargs)
            async with pool.get() as redis:
                await redis.rpush(queue_name, data)

    @staticmethod
    def encode_args(*args):
        return msgpack.packb(args, use_bin_type=True)


def concurrent(func_or_queue):
    dec_queue = None

    def _func_wrapper(func):
        func_name = func.__name__

        if not inspect.iscoroutinefunction(func):
            raise AQError('{} is not a coroutine function'.format(func.__qualname__))
        aq_logger.debug('registering concurrent function %s', func.__qualname__)

        @wraps(func)
        async def _enqueuer(obj, *args, queue_name=None, **kwargs):
            await obj.enqueue_job(func_name, *args, queue_name=queue_name or dec_queue, **kwargs)

        _enqueuer.unbound_original = func
        return _enqueuer

    if isinstance(func_or_queue, str):
        func_or_queue = func_or_queue.encode()

    if isinstance(func_or_queue, bytes):
        dec_queue = func_or_queue
        return _func_wrapper
    else:
        return _func_wrapper(func_or_queue)
