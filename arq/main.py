"""
:mod:`main`
===========

Defines the main ``Actor`` class and ``@concurrent`` decorator for using arq from within your code.

Also defines the ``@cron`` decorator for declaring cron job functions.
"""
import asyncio
import inspect
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union, cast  # noqa

from .jobs import Job
from .utils import RedisMixin, next_cron, to_unix_ms

__all__ = ['Actor', 'concurrent', 'cron']

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

    CRON_SENTINEL_PREFIX = b'arq:cron:'

    #: if not None this name is used instead of the class name when encoding and referencing jobs,
    #: if None the class's name is used
    name: Optional[str] = None

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
                new_v = v.bind(self)
                if isinstance(v, CronJob):
                    yield new_v

    async def enqueue_job(self, func_name: str, *args, queue: str=None, **kwargs) -> Job:
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
        job = self.job_class(class_name=cast(str, self.name), func_name=func_name, args=args, kwargs=kwargs)
        if self._concurrency_enabled:
            redis = self.redis or await self.get_redis()
            main_logger.debug('%s.%s → %s', self.name, func_name, queue)
            await self.job_future(redis, queue, job)
        else:
            main_logger.debug('%s.%s → %s (called directly)', self.name, func_name, queue)
            # encode and decode the job again to make sure it works properly
            job = self.job_class.decode(job.encode(), queue_name=queue, raw_queue=self.queue_lookup[queue])
            await getattr(self, job.func_name).direct(*job.args, **job.kwargs)
        return job

    def job_future(self, redis, queue: str, job):
        return redis.rpush(
            self.queue_lookup[queue],
            job.encode(),
        )

    def _now(self):
        # allow easier mocking
        return datetime.now()

    async def run_cron(self):
        n = self._now()
        to_run = set()

        for cron_job in self.con_jobs:
            if n >= cron_job.next_run:
                to_run.add((cron_job, cron_job.next_run))
                cron_job.set_next(n)

        if not to_run:
            return

        main_logger.debug('cron, %d jobs to run', len(to_run))
        job_futures = set()
        redis_ = self.redis or await self.get_redis()
        with await redis_ as redis:
            for cron_job, run_at in to_run:
                if cron_job.unique:
                    sentinel_key = self.CRON_SENTINEL_PREFIX + f'{self.name}.{cron_job.__name__}'.encode()
                    sentinel_value = str(to_unix_ms(run_at)).encode()
                    v, _ = await asyncio.gather(
                        redis.getset(sentinel_key, sentinel_value),
                        redis.expire(sentinel_key, 3600),
                    )
                    if v == sentinel_value:
                        # if v is equal to sentinel value, another worker has already set it and is doing this cron run
                        continue
                job = self.job_class(class_name=cast(str, self.name), func_name=cron_job.__name__, args=(), kwargs={})
                job_futures.add(self.job_future(redis, cron_job.dft_queue or self.DEFAULT_QUEUE, job))

            job_futures and await asyncio.gather(*job_futures)

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
        return new_inst

    @property
    def bound(self):
        return self._self_obj is not None

    @property
    def dft_queue(self):
        return self._dft_queue

    async def __call__(self, *args, **kwargs):
        return await self.defer(*args, **kwargs)

    async def defer(self, *args, queue_name=None, **kwargs):
        return await self._self_obj.enqueue_job(self._func.__name__, *args, queue=queue_name or self._dft_queue,
                                                **kwargs)

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
    __slots__ = '_func', '_dft_queue', '_self_obj', '_kwargs', 'run_at_startup', 'unique', 'cron_kwargs', 'next_run'

    def __init__(self, *, func, self_obj=None, **kwargs):
        super().__init__(func=func, self_obj=self_obj, **kwargs)
        if not self.bound:
            main_logger.debug('registering cron function %s', func.__qualname__)
        kwargs2 = kwargs.copy()
        self.run_at_startup = kwargs2.pop('run_at_startup')
        self.unique = kwargs2.pop('unique')
        kwargs2.pop('dft_queue')
        self.cron_kwargs = kwargs2
        self.next_run = None
        if self.bound:
            now = self._self_obj._now()
            if self.run_at_startup:
                self.next_run = now
            else:
                self.set_next(now)

    def set_next(self, dt: datetime):
        self.next_run = next_cron(dt, **self.cron_kwargs)

    def __repr__(self):
        return f'<cron function {self._func.__qualname__} of {self._self_obj!r}>'


def cron(*,
         month: Union[None, set, int]=None,
         day: Union[None, set, int]=None,
         weekday: Union[None, set, int, str]=None,
         hour: Union[None, set, int]=None,
         minute: Union[None, set, int]=None,
         second: Union[None, set, int]=0,
         microsecond: int=123456,
         dft_queue=None,
         run_at_startup=False,
         unique=True):
    """
    Decorator which defines a functions as a cron job, eg. it should be executed at specific times.

    Workers will enqueue this job at or just after the set times. If ``unique`` is true (the default) the
    job will only be enqueued once even if multiple workers are running.

    If you wish to call the function directly you can access the original function at ``<func>.direct``.

    :param month: month(s) to run the job on, 1 - 12
    :param day: day(s) to run the job on, 1 - 31
    :param weekday: week day(s) to run the job on, 0 - 6 or mon - sun
    :param hour: hour(s) to run the job on, 0 - 23
    :param minute: minute(s) to run the job on, 0 - 59
    :param second: second(s) to run the job on, 0 - 59
    :param microsecond: microsecond(s) to run the job on,
        defaults to 123456 as the world is busier at the top of a second, 0 - 1e6
    :param dft_queue: default queue to use
    :param run_at_startup: whether to run as worker starts
    :param unique: whether the job should be only be executed once at each time
    """

    return lambda f: CronJob(
        func=f,
        dft_queue=dft_queue,
        run_at_startup=run_at_startup,
        unique=unique,
        month=month,
        day=day,
        weekday=weekday,
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond)
