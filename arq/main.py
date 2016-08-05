import inspect
import logging
from functools import wraps

import msgpack

from .utils import RedisMixin, ellipsis, timestamp

__all__ = ['Actor', 'concurrent', 'Job']

main_logger = logging.getLogger('arq.main')


class Job:
    __slots__ = ('queue', 'queued_at', 'class_name', 'func_name', 'args', 'kwargs')

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
            arguments += ', '.join('{}={!r}'.format(*kv) for kv in sorted(self.kwargs.items()))

        return '{s.class_name}.{s.func_name}({args})'.format(s=self, args=ellipsis(arguments))

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

    def __init__(self, *, is_shadow=False, **kwargs):
        self.queue_lookup = {q: self.QUEUE_PREFIX + q.encode() for q in self.QUEUES}
        self.name = self.name or self.__class__.__name__
        self.is_shadow = is_shadow
        self._bind_direct_methods()
        super().__init__(**kwargs)

    def _bind_direct_methods(self):
        for attr_name in dir(self.__class__):
            unbound_direct = getattr(getattr(self.__class__, attr_name), 'unbound_direct', None)
            if unbound_direct:
                name = attr_name + '_direct'
                if hasattr(self, name):
                    msg = '{} already has a method "{}", this breaks arq direct method binding of "{}"'
                    raise RuntimeError(msg.format(self.name, name, attr_name))
                setattr(self, name, unbound_direct.__get__(self, self.__class__))

    async def enqueue_job(self, func_name, *args, queue=None, **kwargs):
        queue = queue or self.DEFAULT_QUEUE
        data = self.job_class.encode(class_name=self.name, func_name=func_name, args=args, kwargs=kwargs)
        main_logger.debug('%s.%s ▶ %s', self.name, func_name, queue)

        # use the pool directly rather than get_redis_conn to avoid one extra await
        pool = self._redis_pool or await self.get_redis_pool()

        queue_list = self.queue_lookup[queue]
        async with pool.get() as redis:
            await redis.rpush(queue_list, data)

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

        # NOTE: direct is (and has to be) unbound: http://stackoverflow.com/a/7891681/949890
        _enqueuer.unbound_direct = func
        return _enqueuer

    if isinstance(func_or_queue, str):
        dec_queue = func_or_queue
        return _func_wrapper
    else:
        return _func_wrapper(func_or_queue)
