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
            raise AssertionError(f'{cls} looks like it has duplicated queue names: {queues}')
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
    name: str = None

    #: job class to use when encoding and decoding jobs from this actor
    job_class = Job

    #: Whether or not to re-queue jobs if the worker quits before the job has time to finish.
    re_enqueue_jobs = False

    #: queues the actor can enqueue jobs in, order is important, the first queue is highest priority
    queues = (
        HIGH_QUEUE,
        DEFAULT_QUEUE,
        LOW_QUEUE,
    )

    def __init__(self, *args, worker=None, concurrency_enabled=True, **kwargs):
        """
        :param worker: reference to the worker which is managing this actor in shadow mode
        :param concurrency_enabled: **For testing only** if set to False methods are called directly not queued
        :param kwargs: other keyword arguments, see :class:`arq.utils.RedisMixin` for all available options
        """
        self.queue_lookup = {q: self.QUEUE_PREFIX + q.encode() for q in self.queues}
        self.name = self.name or self.__class__.__name__
        self.worker = worker
        self.is_shadow = bool(worker)
        self._bind_concurrent()
        self._concurrency_enabled = concurrency_enabled
        super().__init__(*args, **kwargs)

    async def startup(self):
        """
        Override to setup objects you'll need while running the worker, eg. sessions and database connections
        """
        pass

    async def shutdown(self):
        """
        Override to gracefully close or delete any objects you setup in ``startup``
        """
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
        data = self.job_class.encode(class_name=self.name, func_name=func_name, args=args, kwargs=kwargs)
        main_logger.debug('%s.%s ▶ %s', self.name, func_name, queue)

        if self._concurrency_enabled:
            queue_list = self.queue_lookup[queue]
            # use the pool directly rather than get_redis_conn to avoid one extra await
            pool = self._redis_pool or await self.get_redis_pool()
            async with pool.get() as redis:
                await redis.rpush(queue_list, data)
        else:
            j = self.job_class(data, queue_name=queue)
            await getattr(self, j.func_name).direct(*j.args, **j.kwargs)

    async def close(self, shutdown=False):
        """
        Close down the actor, eg. close the associated redis pool, optionally also calling shutdown.

        :param shutdown: whether or not to also call the shutdown coroutine, you probably only want to set this
          to ``True`` it you called startup previously
        """
        if shutdown:
            await self.shutdown()
        await super().close()

    def __repr__(self):
        return f'<{self.__class__.__name__}({self.name}) at 0x{id(self):02x}>'


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
                raise TypeError(f'{func.__qualname__} is not a coroutine function')

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
        return f'<concurrent function {self._func.__qualname__} of {self._self_obj!r}>'


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
