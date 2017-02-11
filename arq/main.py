"""
:mod:`main`
===========

Defines the main ``Actor`` class and ``@concurrent`` decorator for using arq from within your code.
"""
import inspect
import logging

from .jobs import Job
from .utils import RedisMixin

__all__ = ['Actor', 'concurrent']

main_logger = logging.getLogger('arq.main')


class ActorMeta(type):
    def __new__(mcs, cls, bases, classdict):
        queues = classdict.get('queues')
        if queues and len(queues) != len(set(queues)):
            raise AssertionError('{} looks like it has duplicated queue names: {}'.format(cls, queues))
        return super().__new__(mcs, cls, bases, classdict)


class Actor(RedisMixin, metaclass=ActorMeta):
    """
    All classes which wish to use arq should inherit from Actor.

    Actor defines three default queues: HIGH_QUEUE, DEFAULT_QUEUE and LOW_QUEUE which are processed
    in that order of priority by the worker.

    Actors operate in two modes: normal mode when you initialise them and use them, and "shadow mode"
    where the actor is initialised by the worker and used to execute jobs.
    """
    #: highest priority queue, this can be overwritten by changing :attr:`arq.main.Actor.queues`
    HIGH_QUEUE = 'high'

    #: default queue, this is a special value as it's used in :meth:`arq.main.Actor.enqueue_job`
    DEFAULT_QUEUE = 'dft'

    #: lowest priority queue, can be overwritten
    LOW_QUEUE = 'low'

    #: prefix prepended to all queue names to create the list keys in redis
    QUEUE_PREFIX = b'arq:q:'

    #: if not None this name is used instead of the class name when encoding and referencing jobs,
    #: if None the class's name is used
    name = None  # type: str

    #: job class to use when encoding and decoding jobs from this actor
    job_class = Job

    #: queues the actor can enqueue jobs in, order is important, the first queue is highest priority
    queues = (
        HIGH_QUEUE,
        DEFAULT_QUEUE,
        LOW_QUEUE,
    )

    def __init__(self, *args, is_shadow=False, concurrency_enabled=True, **kwargs):
        """
        :param is_shadow: whether the actor should be in shadow mode, this should only be set by the worker
        :param concurrency_enabled: **For testing only** if set to False methods are called directly not queued
        :param kwargs: other keyword arguments, see :class:`arq.utils.RedisMixin` for all available options
        """
        self.queue_lookup = {q: self.QUEUE_PREFIX + q.encode() for q in self.queues}
        self.name = self.name or self.__class__.__name__
        self.is_shadow = is_shadow
        self._bind_concurrent()
        self._concurrency_enabled = concurrency_enabled
        super().__init__(*args, **kwargs)

    async def startup(self):
        pass

    async def shutdown(self):
        pass

    def _bind_concurrent(self):
        for attr_name in dir(self.__class__):
            v = getattr(self.__class__, attr_name)
            isinstance(v, Concurrent) and v.bind(self)

    async def enqueue_job(self, func_name: str, *args, queue: str=None, **kwargs):
        """
        Enqueue a job by pushing the encoded job information into the redis list specified by the queue.

        Alternatively if concurrency is disabled the job will be encoded, then decoded and ran.
        Disabled concurrency (set via ``disable_concurrency`` init keyword argument) is designed for use in testing,
        hence the job is encoded then decoded to keep tests as close as possible to production.

        :param func_name: name of the function executing the job
        :param args: arguments to pass to the function
        :param queue: name of the queue to use, if None ``DEFAULT_QUEUE`` is used.
        :param kwargs: key word arguments to pass to the function
        """
        queue = queue or self.DEFAULT_QUEUE
        data = self.job_class.encode(class_name=self.name, func_name=func_name,  # type: ignore
                                     args=args, kwargs=kwargs)  # type: ignore
        main_logger.debug('%s.%s ▶ %s', self.name, func_name, queue)

        if self._concurrency_enabled:
            queue_list = self.queue_lookup[queue]
            # use the pool directly rather than get_redis_conn to avoid one extra await
            pool = self._redis_pool or await self.get_redis_pool()
            async with pool.get() as redis:
                await redis.rpush(queue_list, data)
        else:
            j = self.job_class(queue, data)
            await getattr(self, j.func_name).direct(*j.args, **j.kwargs)

    async def close(self):
        await self.shutdown()
        await super().close()

    def __repr__(self):
        return '<{self.__class__.__name__}({self.name}) at 0x{id:02x}>'.format(self=self, id=id(self))


class Concurrent:
    """
    Class used to describe a concurrent function. This is what the ``@concurrent`` decorator returns.

    You shouldn't have to use this directly, but instead apply the ``@concurrent`` decorator
    """
    __slots__ = ['_func', '_dft_queue', '_self_obj']

    def __init__(self, *, func, dft_queue=None, self_obj=None):
        self._self_obj = self_obj
        # if we're already bound we assume func is of the correct type and skip repeat logging
        if not self.bound:
            if not inspect.iscoroutinefunction(func):
                raise TypeError('{} is not a coroutine function'.format(func.__qualname__))

            main_logger.debug('registering concurrent function %s', func.__qualname__)
        self._func = func
        self._dft_queue = dft_queue

    def bind(self, obj: object):
        """
        Equivalent of binding a normal function to an object. A new instance of Concurrent is created and then
        the reference on the parent object updated to that.

        :param obj: object to bind the function to eg. "self" in the eyes of func.
        """
        new_inst = Concurrent(func=self._func, dft_queue=self._dft_queue, self_obj=obj)
        setattr(obj, self._func.__name__, new_inst)

    @property
    def bound(self):
        return self._self_obj is not None

    async def __call__(self, *args, **kwargs):
        return await self.defer(*args, **kwargs)

    async def defer(self, *args, queue_name=None, **kwargs):
        await self._self_obj.enqueue_job(self._func.__name__, *args, queue=queue_name or self._dft_queue, **kwargs)

    async def direct(self, *args, **kwargs):
        return await self._func(self._self_obj, *args, **kwargs)

    @property
    def __doc__(self):
        return self._func.__doc__

    @property
    def __name__(self):
        return self._func.__name__

    def __repr__(self):
        return '<concurrent function {name} of {s!r}>'.format(name=self._func.__qualname__, s=self._self_obj)


def concurrent(func_or_queue):
    """
    Decorator which defines a functions as concurrent, eg. it should be executed on the worker.

    If you wish to call the function directly you can access the original function at ``<func>.direct``.

    The decorator can optionally be used with one argument: the queue to use by default for the job.
    """
    if isinstance(func_or_queue, str):
        return lambda f: Concurrent(func=f, dft_queue=func_or_queue)
    else:
        return Concurrent(func=func_or_queue)
