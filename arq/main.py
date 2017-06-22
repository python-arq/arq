"""
:mod:`main`
===========

Defines the main ``Actor`` class and ``@concurrent`` decorator for using arq from within your code.
"""
import asyncio
import inspect
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Union  # noqa

from .jobs import Job
from .utils import RedisMixin, next_cron

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

    CRON_SENTINAL_PREFIX = b'arq:cron:'

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
        self.con_jobs: List[CronJob] = list(self._bind_decorators())
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

    def _bind_decorators(self):
        for attr_name in dir(self.__class__):
            v = getattr(self.__class__, attr_name)
            if isinstance(v, Bindable):
                v.bind(self)
                if isinstance(v, CronJob):
                    yield v

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
        if self._concurrency_enabled:
            # use the pool directly rather than get_redis_conn to avoid one extra await
            pool = self._redis_pool or await self.get_redis_pool()
            main_logger.debug('%s.%s ▶ %s', self.name, func_name, queue)
            async with pool.get() as redis:
                await self.job_future(redis, queue, func_name, *args, **kwargs)
        else:
            main_logger.debug('%s.%s ▶ %s (called directly)', self.name, func_name, queue)
            data = self.job_class.encode(class_name=self.name, func_name=func_name, args=args, kwargs=kwargs)
            j = self.job_class(data, queue_name=queue)
            await getattr(self, j.func_name).direct(*j.args, **j.kwargs)

    def job_future(self, redis, queue: str, func_name: str, *args, **kwargs):
        return redis.rpush(
            self.queue_lookup[queue],
            self.job_class.encode(class_name=self.name, func_name=func_name, args=args, kwargs=kwargs),
        )

    async def run_cron(self):
        n = datetime.now()
        pool = self._redis_pool or await self.get_redis_pool()
        to_run = set()

        for cron_job in self.con_jobs:
            if n < cron_job.next_run:
                continue
            cron_job.set_next(n)
            to_run.add(cron_job)

        if not to_run:
            return

        async with pool.get() as redis:
            job_futures = set()
            for cron_job in to_run:
                sentinel_key = self.QUEUE_PREFIX + f'{self.name}.{cron_job.__name__}'.encode()
                v, _ = await asyncio.gather(
                    redis.getset(sentinel_key, b'1'),
                    redis.expire(sentinel_key, cron_job.sentinel_timeout),
                )
                if not v:
                    job_futures.add(cron_job.job_future())
            if job_futures:
                await asyncio.gather(*job_futures)

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


class Bindable:
    def __init__(self, *, func, self_obj=None, **kwargs):
        self._self_obj: Actor = self_obj
        # if we're already bound we assume func is of the correct type and skip repeat logging
        if not self.bound and not inspect.iscoroutinefunction(func):
            raise TypeError(f'{func.__qualname__} is not a coroutine function')
        self._func: Callable = func
        self._dft_queue: str = kwargs['dft_queue']
        self._kwargs: Dict[str, Any] = kwargs

    def bind(self, obj: object):
        """
        Equivalent of binding a normal function to an object. A new instance of Concurrent is created and then
        the reference on the parent object updated to that.

        :param obj: object to bind the function to eg. "self" in the eyes of func.
        """
        new_inst = self.__class__(func=self._func, self_obj=obj, **self._kwargs)
        setattr(obj, self._func.__name__, new_inst)

    @property
    def bound(self):
        return self._self_obj is not None

    async def __call__(self, *args, **kwargs):
        return await self.defer(*args, **kwargs)

    async def defer(self, *args, queue_name=None, **kwargs):
        await self._self_obj.enqueue_job(self._func.__name__, *args, queue=queue_name or self._dft_queue, **kwargs)

    def job_future(self, *args, queue_name=None, **kwargs):
        return self._self_obj.job_future(queue_name or self._dft_queue, self._func.__name__, *args, **kwargs)

    async def direct(self, *args, **kwargs):
        return await self._func(self._self_obj, *args, **kwargs)

    @property
    def __name__(self):
        return self._func.__name__


class Concurrent(Bindable):
    """
    Class used to describe a concurrent function. This is what the ``@concurrent`` decorator returns.

    You shouldn't have to use this directly, but instead apply the ``@concurrent`` decorator
    """
    __slots__ = '_func', '_dft_queue', '_self_obj', '_kwargs'

    def __init__(self, *, func, self_obj=None, dft_queue=None):
        super().__init__(func=func, self_obj=self_obj, dft_queue=dft_queue)
        if not self.bound:
            main_logger.debug('registering concurrent function %s', func.__qualname__)

    @property
    def __doc__(self):
        return self._func.__doc__

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


class CronJob(Bindable):
    def __init__(self, *, func, self_obj=None, **kwargs):
        super().__init__(func=func, self_obj=self_obj, **kwargs)
        if not self.bound:
            main_logger.debug('registering cron function %s', func.__qualname__)
        kwargs2 = kwargs.copy()
        self.run_at_startup = kwargs2.pop('run_at_startup')
        self.sentinel_timeout = kwargs2.pop('sentinel_timeout')
        kwargs2.pop('dft_queue')
        self.cron_kwargs = kwargs2
        self.next_run = None
        self.set_next(datetime.now())

    def set_next(self, dt: datetime):
        self.next_run = next_cron(dt, **self.cron_kwargs)


def cron(*,
         dft_queue=None,
         run_at_startup=False,
         sentinel_timeout=10,
         month: Union[None, set, int]=None,
         day: Union[None, set, int]=None,
         weekday: Union[None, set, int, str]=None,
         hour: Union[None, set, int]=None,
         minute: Union[None, set, int]=None,
         second: Union[None, set, int]=0,
         microsecond: int=123456):

    return lambda f: CronJob(
        func=f,
        dft_queue=dft_queue,
        run_at_startup=run_at_startup,
        sentinel_timeout=sentinel_timeout,
        month=month,
        day=day,
        weekday=weekday,
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond)
