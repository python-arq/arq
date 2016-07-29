import asyncio
import inspect
import logging
from functools import wraps
from traceback import format_exception
import sys

import msgpack

from .utils import RedisMixin, timestamp


__all__ = ['Actor', 'concurrent', 'arq_mode', 'Job']

main_logger = logging.getLogger('arq.main')
work_logger = logging.getLogger('arq.work')


class ArqMode:
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

arq_mode = ArqMode()


class Job:
    def __init__(self, queue, data):
        self.queue = queue
        data = msgpack.unpackb(data, encoding='utf8')
        self.queued_at, self.class_name, self.func_name, self.args, self.kwargs = data
        self.queued_at /= 1000


class ActorMeta(type):
    __doc__ = 'arq Actor'

    def __new__(mcs, cls, bases, classdict):
        queues = classdict.get('QUEUES')
        if queues and len(queues) != len(set(queues)):
            raise AssertionError('{} looks like it has duplicated queue names: {}'.format(cls.__name__, queues))
        return super().__new__(mcs, cls, bases, classdict)


class Actor(RedisMixin, metaclass=ActorMeta):
    HIGH_QUEUE = 'high'
    DEFAULT_QUEUE = 'dft'
    LOW_QUEUE = 'low'
    QUEUE_PREFIX = b'arq:q:'

    QUEUES = (
        HIGH_QUEUE,
        DEFAULT_QUEUE,
        LOW_QUEUE,
    )

    def __init__(self, **kwargs):
        self.arq_tasks = set()
        self.queue_lookup = {q: self.QUEUE_PREFIX + q.encode() for q in self.QUEUES}
        super().__init__(**kwargs)

    async def enqueue_job(self, func_name, *args, queue=None, **kwargs):
        queue = queue or self.DEFAULT_QUEUE
        data = self.encode_job(
            func_name=func_name,
            args=args,
            kwargs=kwargs,
        )
        main_logger.debug('%s.%s ▶ %s (mode: %s)', self.__class__.__name__, func_name, queue, arq_mode)

        if arq_mode.direct or arq_mode.asyncio_loop:
            job = Job(queue, data)
            coro = self.run_job(job)
            if arq_mode.direct:
                await coro
            else:
                self.arq_tasks.add(self.loop.create_task(coro))
        else:
            pool = await self.init_redis_pool()

            queue_list = self.queue_lookup[queue]
            async with pool.get() as redis:
                await redis.rpush(queue_list, data)

    def encode_job(self, *, func_name, args, kwargs):
        queued_at = int(timestamp() * 1000)
        return msgpack.packb([queued_at, self.__class__.__name__, func_name, args, kwargs], use_bin_type=True)

    async def close(self):
        if arq_mode.asyncio_loop:
            await asyncio.wait(self.arq_tasks, loop=self.loop)
        await super().close()

    @classmethod
    def log_job_start(cls, queue_time, j):
        if not work_logger.isEnabledFor(logging.INFO):
            return
        arguments = ''
        if j.args:
            arguments = ', '.join(map(str, j.args))
        if j.kwargs:
            if arguments:
                arguments += ', '
            arguments += ', '.join('{}={}'.format(*kv) for kv in j.kwargs.items())

        if len(arguments) > 80:
            arguments = arguments[:77] + '...'
        work_logger.info('%-4s queued%7.3fs → %s.%s(%s)', j.queue, queue_time, j.class_name, j.func_name, arguments)

    @classmethod
    def log_job_result(cls, started_at, result, j):
        if not work_logger.isEnabledFor(logging.INFO):
            return
        job_time = timestamp() - started_at
        if result is None:
            sr = ''
        else:
            sr = str(result)
            if len(sr) > 80:
                sr = sr[:77] + '...'
        work_logger.info('%-4s ran in%7.3fs ← %s.%s ● %s', j.queue, job_time, j.class_name, j.func_name, sr)

    @classmethod
    async def handle_exc(cls, started_at, exc, j):
        job_time = timestamp() - started_at
        work_logger.error('%-4s ran in =%7.3fs ! %s.%s: %s',
                          j.queue, job_time, j.class_name, j.func_name, exc.__class__.__name__)
        tb = format_exception(*sys.exc_info())
        work_logger.error(''.join(tb).strip('\n'))

    async def run_job(self, j):
        func = getattr(self, j.func_name)

        started_at = timestamp()
        queue_time = started_at - j.queued_at
        self.log_job_start(queue_time, j)
        unbound_func = getattr(func, 'unbound_original', None)
        try:
            if unbound_func:
                result = await unbound_func(self, *j.args, **j.kwargs)
            else:
                result = await func(*j.args, **j.kwargs)
        except Exception as e:
            await self.handle_exc(started_at, e, j)
        else:
            self.log_job_result(started_at, result, j)


def concurrent(func_or_queue):
    dec_queue = None

    def _func_wrapper(func):
        func_name = func.__name__

        if not inspect.iscoroutinefunction(func):
            raise TypeError('{} is not a coroutine function'.format(func.__qualname__))
        main_logger.debug('registering concurrent function %s', func.__qualname__)

        @wraps(func)
        async def _enqueuer(obj, *args, queue_name=None, **kwargs):
            await obj.enqueue_job(func_name, *args, queue=queue_name or dec_queue, **kwargs)

        _enqueuer.unbound_original = func
        return _enqueuer

    if isinstance(func_or_queue, str):
        dec_queue = func_or_queue
        return _func_wrapper
    else:
        return _func_wrapper(func_or_queue)
