import inspect
import logging
from functools import wraps
from traceback import format_exception
import sys

import msgpack

from .utils import RedisMixin, timestamp


__all__ = ['Actor', 'concurrent', 'Job']

main_logger = logging.getLogger('arq.main')
work_logger = logging.getLogger('arq.work')


class Job:
    def __init__(self, queue, data):
        self.queue = queue
        data = msgpack.unpackb(data, encoding='utf8')
        self.queued_at, self.class_name, self.func_name, self.args, self.kwargs = data
        self.queued_at /= 1000

    @classmethod
    def encode(cls, *, queued_at=None, class_name, func_name, args, kwargs):
        queued_at = queued_at or int(timestamp() * 1000)
        return msgpack.packb([queued_at, class_name, func_name, args, kwargs], use_bin_type=True)

    def __str__(self):
        arguments = ''
        if self.args:
            arguments = ', '.join(map(str, self.args))
        if self.kwargs:
            if arguments:
                arguments += ', '
            arguments += ', '.join('{}={}'.format(*kv) for kv in sorted(self.kwargs.items()))

        if len(arguments) > 80:
            arguments = arguments[:77] + '...'
        return '{s.class_name}.{s.func_name}({args})'.format(s=self, args=arguments)

    def __repr__(self):
        return '<Job {} on {}>'.format(self, self.queue)


class ActorMeta(type):
    __doc__ = 'arq Actor'

    def __new__(mcs, cls, bases, classdict):
        queues = classdict.get('QUEUES')
        if queues and len(queues) != len(set(queues)):
            raise AssertionError('{} looks like it has duplicated queue names: {}'.format(cls, queues))
        return super().__new__(mcs, cls, bases, classdict)


class Actor(RedisMixin, metaclass=ActorMeta):
    HIGH_QUEUE = 'high'
    DEFAULT_QUEUE = 'dft'
    LOW_QUEUE = 'low'
    QUEUE_PREFIX = b'arq:q:'
    name = None
    job_class = Job

    QUEUES = (
        HIGH_QUEUE,
        DEFAULT_QUEUE,
        LOW_QUEUE,
    )

    def __init__(self, *, name=None, **kwargs):
        self.queue_lookup = {q: self.QUEUE_PREFIX + q.encode() for q in self.QUEUES}
        if name or not self.name:
            self.name = name or self.__class__.__name__
        super().__init__(**kwargs)

    async def enqueue_job(self, func_name, *args, queue=None, **kwargs):
        queue = queue or self.DEFAULT_QUEUE
        data = self.job_class.encode(class_name=self.name, func_name=func_name, args=args, kwargs=kwargs)
        main_logger.debug('%s.%s ▶ %s', self.name, func_name, queue)

        pool = await self.get_redis_pool()

        queue_list = self.queue_lookup[queue]
        async with pool.get() as redis:
            await redis.rpush(queue_list, data)

    @classmethod
    def log_job_start(cls, queue_time, j):
        if work_logger.isEnabledFor(logging.INFO):
            work_logger.info('%-4s queued%7.3fs → %s', j.queue, queue_time, j)

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
            return 1
        else:
            self.log_job_result(started_at, result, j)
            return 0

    def __repr__(self):
        return '<{self.__class__.__name__}({self.name}) at 0x{id:02x}>'.format(self=self, id=id(self))


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
